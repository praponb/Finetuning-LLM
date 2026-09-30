WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        HOUR(event_time) AS event_time_hour,
        COALESCE(page, 'NONE') AS page_filled
    FROM web_events
    WHERE event_time > DATE '2023-10-25'
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM filtered
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM gaps
)
-- Grouped metrics
SELECT
    event_type,
    SUM(duration_sec) AS total_duration_sec,
    AVG(session_id_2) FILTER (WHERE page = '/checkout') AS checkout_avg_session_id_2
FROM islands
GROUP BY 1
HAVING SUM(duration_sec) >= 0
ORDER BY total_duration_sec NULLS FIRST
