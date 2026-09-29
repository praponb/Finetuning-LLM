WITH with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(started_at) OVER (PARTITION BY device ORDER BY started_at, id) AS prev_started_at
    FROM sessions
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_started_at IS NULL OR started_at > prev_started_at + INTERVAL '90' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
)
-- Multi-level aggregation
SELECT
    CASE WHEN new_group_flag >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END AS new_group_flag_tier,
    COUNT(*) FILTER (WHERE country = 'TH') AS th_rows,
    COUNT(*) FILTER (WHERE country = 'US') AS us_rows,
    (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 AS span_minutes,
    COUNT_IF(browser = 'Firefox') AS firefox_events,
    GROUPING_ID() AS gid
FROM sessionized
GROUP BY ROLLUP (CASE WHEN new_group_flag >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END)
HAVING th_rows >= 1
ORDER BY us_rows NULLS FIRST
