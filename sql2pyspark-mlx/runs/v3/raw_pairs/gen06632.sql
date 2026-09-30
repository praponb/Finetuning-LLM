SELECT
    id,
    txn_time,
    DATEDIFF(CURRENT_DATE(), txn_time) AS days_since_txn_time,
    FLOOR(amount) AS amount_whole
FROM transactions
WHERE is_flagged = TRUE
