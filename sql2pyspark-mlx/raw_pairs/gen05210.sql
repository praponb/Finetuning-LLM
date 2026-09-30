WITH with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) > INTERVAL 30 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM web_events
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
)
-- Aggregate with conditional metrics
SELECT
    event_type,
    DATE_TRUNC('QUARTER', event_time) AS quarter_start,
    CASE WHEN duration_sec >= 374 THEN 'PREMIUM' ELSE 'STANDARD' END AS duration_sec_tier,
    MAX(event_time) AS last_event_time,
    SUM(island_id) FILTER (WHERE page = '/checkout') AS checkout_total_island_id,
    SORT_ARRAY(COLLECT_SET(event_type)) AS event_type_list
FROM islands
GROUP BY CUBE (event_type, DATE_TRUNC('QUARTER', event_time), CASE WHEN duration_sec >= 374 THEN 'PREMIUM' ELSE 'STANDARD' END)
HAVING checkout_total_island_id > 0;
