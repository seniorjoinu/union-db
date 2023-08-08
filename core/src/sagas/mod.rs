use async_trait::async_trait;
use candid::Principal;
use ic_cdk::{
    api::call::{call_raw, RejectionCode},
    call, spawn,
};

pub const REDIRECT_METHOD_NAME: &str = "union_db_execute_redirected";

pub type ShardId = Principal;

#[async_trait]
pub trait Transaction {
    async fn run(&mut self) -> Option<ShardId>;
}

#[derive(Clone, Copy)]
pub struct Dispatcher {
    current_shard_id: ShardId,
}

impl Dispatcher {
    pub fn run_saga<'a, T: Transaction + 'static>(&'a self, saga: T) {
        let s = Box::new(saga);
        let this = self.clone();

        spawn(async move { this.process_boxed_saga(s).await });
    }

    async fn process_boxed_saga<T: Transaction>(&self, mut saga: Box<T>) {
        let mut saga_result = saga.run().await;

        while let Some(next_shard_id) = saga_result {
            if next_shard_id == self.current_shard_id {
                saga_result = saga.run().await;
            } else {
                redirect(saga, next_shard_id).await;
                break;
            }
        }
    }
}

pub async fn redirect<T: Transaction>(saga: Box<T>, canister_id: Principal) {
    let saga_blob = Vec::new();

    let mut result = call_raw(canister_id, REDIRECT_METHOD_NAME, &saga_blob, 0).await;

    loop {
        match result {
            Ok(empty_blob) => {
                assert!(empty_blob.is_empty());

                break;
            }
            Err((code, msg)) => match code {
                RejectionCode::CanisterError => panic!("{msg}"),
                _ => {
                    result = call_raw(canister_id, REDIRECT_METHOD_NAME, &saga_blob, 0).await;
                }
            },
        }
    }
}
