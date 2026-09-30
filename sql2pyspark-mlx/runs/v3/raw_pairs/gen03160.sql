SELECT
    merchant,
    txn_type,
    TO_DATE(txn_time) AS txn_day,
    MIN(amount) AS min_amount
FROM transactions
WHERE NOT is_flagged AND merchant = 'Starbucks'
GROUP BY merchant, txn_type, txn_day
ORDER BY txn_day, merchant DESC
