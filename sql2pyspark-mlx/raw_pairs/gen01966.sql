select a.customer_id, a.account_type, t.is_flagged, t.amount, t.txn_time from accounts a left join transactions t on t.account_id = a.id where not t.is_flagged
