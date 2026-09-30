WITH prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) AS prev_paid_at
    FROM payments
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM prev_events
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        method,
        amount,
        paid_at,
        status,
        SUM(is_new_session) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
)
-- Grouped metrics
SELECT
    DATE_TRUNC('QUARTER', paid_at) AS quarter_start,
    SIZE(COLLECT_SET(method)) AS num_methods,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    SORT_ARRAY(COLLECT_SET(method)) AS method_list
FROM sessionized
GROUP BY quarter_start
