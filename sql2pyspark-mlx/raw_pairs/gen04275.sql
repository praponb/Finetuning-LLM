SELECT
    id,
    is_flagged,
    amount,
    HOUR(txn_time) AS txn_time_hour,
    DATE_TRUNC('MONTH', txn_time) AS txn_time_period
FROM transactions
WHERE is_flagged = TRUE
ORDER BY amount DESC, id
LIMIT 20;
