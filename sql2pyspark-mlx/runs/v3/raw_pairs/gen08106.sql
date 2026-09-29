SELECT a.account_type, MIN(t.txn_time) AS first_txn_time FROM transactions t INNER JOIN accounts a ON a.id = t.account_id WHERE a.customer_id IS NOT NULL GROUP BY a.account_type ORDER BY account_type
