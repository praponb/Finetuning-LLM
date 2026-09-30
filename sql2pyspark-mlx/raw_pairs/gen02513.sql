SELECT merchant, txn_type
FROM transactions
WHERE txn_time >= '2024-06-01'
UNION ALL
SELECT merchant, txn_type
FROM transactions
WHERE txn_type NOT IN ('REFUND', 'FEE');
