WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN merchant IN ('Starbucks', 'Lazada') THEN 'Priority' ELSE 'Other' END AS merchant_group
    FROM transactions
    WHERE txn_type NOT IN ('CREDIT', 'REFUND', 'DEBIT')
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) AS prev_txn_time
    FROM filtered
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_txn_time IS NULL OR txn_time > prev_txn_time + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
)
-- Grouped metrics
SELECT
    CASE WHEN session_no >= 9 THEN 'LARGE' ELSE 'SMALL' END AS session_no_tier,
    merchant,
    DATE_TRUNC('QUARTER', prev_txn_time) AS quarter_start,
    (UNIX_TIMESTAMP(MAX(prev_txn_time)) - UNIX_TIMESTAMP(MIN(prev_txn_time))) / 60 AS duration_min,
    SORT_ARRAY(COLLECT_SET(txn_type)) AS txn_type_list,
    MIN(txn_time) AS first_seen,
    ROUND(SUM(is_new_session) / NULLIF(COUNT(CASE WHEN txn_type = 'CREDIT' THEN 1 END), 0), 2) AS is_new_session_per_credit,
    COUNT(*) AS num_rows
FROM sessionized
GROUP BY session_no_tier, merchant, quarter_start
HAVING duration_min >= 0;
