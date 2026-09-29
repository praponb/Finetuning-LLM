SELECT
    id,
    account_id,
    merchant,
    txn_time,
    CEIL(amount) AS amount_whole,
    TO_DATE(txn_time) AS txn_time_date
FROM transactions
WHERE NOT is_flagged OR txn_type = 'REFUND'
ORDER BY txn_time_date
