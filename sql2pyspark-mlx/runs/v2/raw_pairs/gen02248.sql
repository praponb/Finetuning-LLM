select t.id, t.txn_time, t.txn_type, t.amount from transactions t where t.account_id not in (select id from accounts where balance > 50000)
