WITH prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) AS prev_event_time
    FROM web_events
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM prev_events
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
)
-- Multi-level aggregation
SELECT
    CASE WHEN is_new_burst >= 1 THEN 'HIGH' ELSE 'LOW' END AS is_new_burst_tier,
    session_id,
    DATE_TRUNC('QUARTER', event_time) AS quarter_start,
    SIZE(COLLECT_SET(page)) AS num_pages,
    SIZE(COLLECT_SET(event_type)) AS num_event_types,
    SIZE(COLLECT_SET(event_type)) AS num_event_types_2,
    COUNT(*) AS event_count,
    GROUPING(CASE WHEN is_new_burst >= 1 THEN 'HIGH' ELSE 'LOW' END) AS grouping_level
FROM sessionized
GROUP BY CUBE (CASE WHEN is_new_burst >= 1 THEN 'HIGH' ELSE 'LOW' END, session_id, DATE_TRUNC('QUARTER', event_time))
ORDER BY num_pages NULLS LAST
