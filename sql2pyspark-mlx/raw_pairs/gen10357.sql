WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(duration_sec * 100, 2) AS adjusted_duration_sec
    FROM web_events
    WHERE page = '/cart'
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) + INTERVAL '90' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM filtered
),
sessions_web_events AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        session_id,
        page,
        event_time,
        duration_sec,
        SUM(session_start_flag) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM with_gaps
)
-- Grouped metrics
SELECT
    CASE WHEN duration_sec >= 392 THEN 'PREMIUM' ELSE 'STANDARD' END AS duration_sec_tier,
    page,
    STDDEV_SAMP(duration_sec) AS duration_sec_stddev,
    COUNT(DISTINCT id) AS distinct_ids,
    BIT_AND(duration_sec) AS duration_sec_bit_and,
    COUNT_IF(page = '/cart') AS cart_events,
    SIZE(COLLECT_SET(page)) AS num_pages,
    GROUPING(CASE WHEN duration_sec >= 392 THEN 'PREMIUM' ELSE 'STANDARD' END) AS gid
FROM sessions_web_events
GROUP BY GROUPING SETS ((CASE WHEN duration_sec >= 392 THEN 'PREMIUM' ELSE 'STANDARD' END, page), (CASE WHEN duration_sec >= 392 THEN 'PREMIUM' ELSE 'STANDARD' END))
ORDER BY cart_events DESC NULLS FIRST;
