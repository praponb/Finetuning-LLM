WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at ASC, id ASC) > INTERVAL '14' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM payments
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY order_id ORDER BY paid_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
)
-- Aggregate with conditional metrics
SELECT
    status,
    ROUND(SUM(session_id) / NULLIF(COUNT(CASE WHEN method = 'card' THEN 1 END), 0), 2) AS session_id_per_card,
    COUNT(*) AS cnt,
    SIZE(COLLECT_SET(method)) AS num_methods,
    COUNT(CASE WHEN method = 'bank_transfer' THEN 1 END) AS bank_transfer_count,
    COUNT(DISTINCT status) AS distinct_status,
    COUNT(DISTINCT id) AS distinct_ids
FROM islands
GROUP BY status
