WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) + INTERVAL '7' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM web_events
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('QUARTER', event_time) AS quarter_start,
    DATE_TRUNC('YEAR', event_time) AS year_start,
    COUNT(CASE WHEN page = '/product' THEN 1 END) AS product_count,
    BIT_AND(duration_sec) AS duration_sec_bit_and,
    SIZE(COLLECT_SET(event_type)) AS num_event_types
FROM sessionized
GROUP BY CUBE (DATE_TRUNC('QUARTER', event_time), DATE_TRUNC('YEAR', event_time))
