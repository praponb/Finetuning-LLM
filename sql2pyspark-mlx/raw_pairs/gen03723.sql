SELECT txn_type, AVG(amount) AS avg_amount FROM transactions WHERE account_id IN (4, 11, 26, 35) GROUP BY txn_type ORDER BY avg_amount DESC
