WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, TO_DATE(txn_time) AS txn_time_date
    FROM transactions
    WHERE txn_time > TO_TIMESTAMP('2023-09-28 13:00:00')
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) AS prev_txn_time
    FROM valid
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_txn_time IS NULL OR txn_time > prev_txn_time + INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM with_prev
),
sessions_transactions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('YEAR', prev_txn_time) AS year_start,
    CASE WHEN amount >= 3200 THEN 'PREMIUM' ELSE 'STANDARD' END AS amount_tier,
    APPROX_PERCENTILE(is_new_burst, 0.9) AS is_new_burst_p90,
    COUNT(*) AS num_rows,
    COUNT(*) FILTER (WHERE txn_type = 'FEE') AS fee_rows
FROM sessions_transactions
GROUP BY ROLLUP (DATE_TRUNC('YEAR', prev_txn_time), CASE WHEN amount >= 3200 THEN 'PREMIUM' ELSE 'STANDARD' END)
ORDER BY year_start DESC;
