SELECT t.txn_time, a.customer_id, a.account_type FROM transactions t INNER JOIN accounts a ON t.account_id = a.id ORDER BY customer_id DESC
