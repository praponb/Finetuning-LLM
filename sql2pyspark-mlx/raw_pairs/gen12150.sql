SELECT txn_type
FROM transactions
WHERE txn_time < '2025-08-01'
UNION ALL
SELECT txn_type
FROM transactions
WHERE txn_type IN ('DEBIT', 'REFUND')
