WITH with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) AS prev_txn_time
    FROM transactions
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_txn_time IS NULL OR txn_time > prev_txn_time + INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM with_prev
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
)
-- Grouped metrics
SELECT
    merchant,
    COUNT(CASE WHEN txn_type = 'DEBIT' THEN 1 END) AS debit_count,
    SIZE(COLLECT_SET(merchant)) AS num_merchants,
    MIN(txn_time) AS first_txn_time,
    SORT_ARRAY(COLLECT_SET(txn_type)) AS txn_type_list,
    SIZE(COLLECT_SET(txn_type)) AS num_txn_types,
    PERCENTILE_APPROX(amount, ARRAY(0.25, 0.5, 0.75)) AS amount_quartiles
FROM islands
GROUP BY merchant
HAVING debit_count < 1000
ORDER BY num_merchants DESC
