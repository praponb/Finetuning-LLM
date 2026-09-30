select a.account_type, a.customer_id, a.balance, t.merchant, t.txn_type, t.txn_time from accounts a left outer join transactions t on a.id = t.account_id order by t.txn_time
