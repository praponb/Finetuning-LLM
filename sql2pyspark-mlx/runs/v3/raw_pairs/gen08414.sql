SELECT account_id
FROM transactions
WHERE txn_time >= '2025-08-14'
UNION ALL
SELECT account_id
FROM transactions
WHERE amount >= 3100
