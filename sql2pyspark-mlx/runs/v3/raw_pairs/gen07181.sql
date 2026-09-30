select t.id, t.txn_type, t.amount, t.account_id from transactions t where t.amount < (select avg(t2.amount) from transactions t2 where t2.txn_type = t.txn_type) order by t.id desc
