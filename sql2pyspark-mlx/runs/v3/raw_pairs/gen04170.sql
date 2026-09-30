SELECT merchant, AVG(amount) AS avg_amount FROM transactions WHERE NOT (txn_time < '2025-09-11' AND (TO_DATE(txn_time) < '2024-08-01' OR txn_type IN ('FEE', 'DEBIT'))) GROUP BY 1
