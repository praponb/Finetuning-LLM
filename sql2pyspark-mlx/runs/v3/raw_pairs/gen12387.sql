select a.customer_id, t.txn_type, t.txn_time from accounts a join transactions t on a.id = t.account_id order by txn_type, txn_time desc;
