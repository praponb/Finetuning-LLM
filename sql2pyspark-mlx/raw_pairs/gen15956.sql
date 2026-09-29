WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) > INTERVAL '90' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM sessions
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        SUM(new_group_flag) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
)
-- Multi-level aggregation
SELECT
    burst_id,
    COUNT_IF(browser = 'Firefox') AS firefox_events,
    MIN(started_at) AS first_started_at,
    COUNT(*) FILTER (WHERE device = 'tablet') AS tablet_rows
FROM sessionized
GROUP BY 1
HAVING COUNT_IF(browser = 'Firefox') > 1
ORDER BY firefox_events NULLS FIRST
