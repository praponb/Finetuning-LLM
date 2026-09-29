SELECT
    id,
    account_id,
    txn_time,
    CAST(is_flagged AS INT) AS is_flagged_flag
FROM transactions
WHERE NOT ((NOT is_flagged AND HOUR(txn_time) BETWEEN 12 AND 19) OR TO_DATE(txn_time) < '2023-04-01')
ORDER BY is_flagged_flag DESC, 2 DESC;
