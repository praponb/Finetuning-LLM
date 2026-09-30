select t.id, t.account_id, t.amount, t.txn_time from transactions t where t.amount = (select max(t2.amount) from transactions t2 where t2.account_id = t.account_id)
