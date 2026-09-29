WITH with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) > INTERVAL 14 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM sessions
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        customer_id,
        device,
        started_at,
        country,
        SUM(is_new_session) OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('WEEK', started_at) AS week_start,
    COUNT(*) AS cnt,
    SUM(session_id) FILTER (WHERE device = 'desktop') AS desktop_total_session_id,
    SORT_ARRAY(COLLECT_SET(device)) AS device_list,
    COUNT(CASE WHEN device = 'mobile' THEN 1 END) AS mobile_count,
    SORT_ARRAY(COLLECT_SET(country)) AS country_list,
    SORT_ARRAY(COLLECT_SET(country)) AS country_list_2
FROM islands
GROUP BY 1
