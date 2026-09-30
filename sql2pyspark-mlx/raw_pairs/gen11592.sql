WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) + INTERVAL '14' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM sessions
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
)
-- Aggregate with conditional metrics
SELECT
    browser,
    DATE_TRUNC('QUARTER', started_at) AS quarter_start,
    COUNT_IF(browser = 'Firefox') AS firefox_events,
    (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 AS span_minutes,
    SUM(CASE WHEN device = 'desktop' THEN new_group_flag ELSE 0 END) AS desktop_new_group_flag,
    COUNT(CASE WHEN device = 'mobile' THEN 1 END) AS mobile_count,
    AVG(new_group_flag) AS avg_new_group_flag,
    SIZE(COLLECT_SET(country)) AS num_countries,
    GROUPING_ID() AS gid
FROM sessionized
GROUP BY ROLLUP (browser, DATE_TRUNC('QUARTER', started_at))
HAVING COUNT_IF(browser = 'Firefox') < 1000;
