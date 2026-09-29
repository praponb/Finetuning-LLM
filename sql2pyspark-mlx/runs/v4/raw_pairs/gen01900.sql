WITH with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY page ORDER BY event_time ASC, id ASC) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY page ORDER BY event_time ASC, id ASC) + INTERVAL '14' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM web_events
),
sessions_web_events AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY page ORDER BY event_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
)
-- Grouped metrics
SELECT
    DATE_TRUNC('YEAR', event_time) AS year_start,
    COUNT(*) FILTER (WHERE event_type = 'search') AS search_rows,
    COUNT(DISTINCT page) AS distinct_pages,
    ROUND(AVG(duration_sec), 2) AS avg_duration_sec,
    COUNT(DISTINCT page) AS distinct_pages_2,
    BIT_OR(duration_sec) AS duration_sec_bit_or,
    SIZE(COLLECT_SET(page)) AS num_pages
FROM sessions_web_events
GROUP BY DATE_TRUNC('YEAR', event_time)
ORDER BY distinct_pages_2 ASC NULLS LAST;
