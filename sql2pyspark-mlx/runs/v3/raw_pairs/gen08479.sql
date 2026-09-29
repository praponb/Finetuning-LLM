SELECT a.account_type, a.customer_id, a.opened_date, t.txn_type, t.txn_time, t.is_flagged FROM accounts a INNER JOIN transactions t ON a.id = t.account_id ORDER BY a.opened_date ASC, customer_id DESC
