WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        FLOOR(duration_sec) AS duration_sec_whole,
        COALESCE(page, 'UNKNOWN') AS page_filled
    FROM web_events
    WHERE event_time > TIMESTAMP '2023-10-03 20:00:00'
      AND session_id = 1
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM valid
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('YEAR', event_time) AS year_start,
    SUM(CASE WHEN page = '/search' THEN session_no ELSE 0 END) AS search_session_no,
    COUNT(CASE WHEN page = '/checkout' THEN 1 END) AS checkout_count,
    PERCENTILE_APPROX(duration_sec_whole, ARRAY(0.25, 0.5, 0.75)) AS duration_sec_whole_quartiles,
    AVG(duration_sec_whole) FILTER (WHERE event_type = 'search') AS search_avg_duration_sec_whole
FROM islands
GROUP BY year_start
ORDER BY search_session_no DESC NULLS FIRST
