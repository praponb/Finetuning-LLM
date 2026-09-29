SELECT
    id,
    merchant,
    account_id,
    DAY(txn_time) AS txn_time_day
FROM transactions
WHERE account_id IN (9, 16, 23, 30)
ORDER BY txn_time_day, id
LIMIT 1
