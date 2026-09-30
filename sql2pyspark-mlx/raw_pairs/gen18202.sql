WITH with_prev AS (
    SELECT *, LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) AS prev_txn_time
    FROM transactions
),
with_gaps AS (
    SELECT *, CASE WHEN prev_txn_time IS NULL OR txn_time > prev_txn_time + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
islands AS (
    SELECT
        id,
        account_id,
        txn_type,
        txn_time,
        merchant,
        is_flagged,
        SUM(is_new_session) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
)
SELECT
    txn_type,
    SORT_ARRAY(COLLECT_SET(txn_type)) AS txn_type_list,
    SIZE(COLLECT_SET(txn_type)) AS num_txn_types
FROM islands
GROUP BY 1
