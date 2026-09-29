SELECT * FROM (SELECT txn_type, merchant, amount FROM transactions WHERE txn_time >= '2025-01-01') PIVOT (MAX(amount) FOR merchant IN ('Amazon', 'Lazada'))
