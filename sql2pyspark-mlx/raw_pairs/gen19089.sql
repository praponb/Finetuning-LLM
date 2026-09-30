WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time ASC, id ASC) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time ASC, id ASC) + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM transactions
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        is_flagged,
        SUM(session_start_flag) OVER (PARTITION BY account_id ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN amount >= 3000 THEN 'PREMIUM' ELSE 'STANDARD' END AS amount_tier,
    MIN(txn_time) AS first_seen,
    AVG(amount) FILTER (WHERE txn_type = 'FEE') AS fee_avg_amount,
    COUNT(*) AS row_count,
    SORT_ARRAY(COLLECT_SET(txn_type)) AS txn_type_list,
    COUNT_IF(txn_type = 'REFUND') AS refund_events,
    AVG(amount) AS avg_amount
FROM islands
GROUP BY CASE WHEN amount >= 3000 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY refund_events DESC;
