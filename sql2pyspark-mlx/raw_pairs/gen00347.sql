WITH with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM payments
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
)
-- Grouped metrics
SELECT
    island_id,
    status,
    ROUND(AVG(amount), 2) AS avg_amount,
    SIZE(COLLECT_SET(status)) AS num_status,
    (UNIX_TIMESTAMP(MAX(paid_at)) - UNIX_TIMESTAMP(MIN(paid_at))) / 60 AS span_minutes,
    GROUPING_ID() AS gid
FROM sessionized
GROUP BY CUBE (island_id, status)
ORDER BY span_minutes DESC NULLS FIRST
