WITH remaining_keys AS (
    SELECT account_id, merchant
    FROM transactions
    EXCEPT
    SELECT account_id, merchant
    FROM transactions
    WHERE amount < 1600
),
verified AS (
    SELECT s.*
    FROM transactions s
    JOIN remaining_keys k ON s.account_id = k.account_id AND s.merchant = k.merchant
)
SELECT
    DATE_TRUNC('MONTH', txn_time) AS activity_month,
    account_id,
    COUNT_IF(txn_type = 'FEE') AS fee_events,
    COUNT(CASE WHEN txn_type = 'FEE' THEN 1 END) AS fee_count,
    COUNT(*) AS num_rows,
    GROUPING(DATE_TRUNC('MONTH', txn_time)) AS grouping_level
FROM verified
GROUP BY ROLLUP (DATE_TRUNC('MONTH', txn_time), account_id)
ORDER BY fee_events;
