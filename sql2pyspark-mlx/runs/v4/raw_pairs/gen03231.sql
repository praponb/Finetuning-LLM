WITH latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM web_events
    QUALIFY ROW_NUMBER() OVER (PARTITION BY session_id, event_type ORDER BY duration_sec DESC, id ASC) = 1
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time ASC, id ASC) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time ASC, id ASC) + INTERVAL '90' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM latest
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY event_type ORDER BY event_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, duration_sec * session_no duration_sec_times_session_no
    FROM islands
    WHERE event_time > TIMESTAMP '2024-03-07 18:00:00'
      AND session_id IN (15, 20, 31, 33)
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN duration_sec_times_session_no >= 77 THEN 'PREMIUM' ELSE 'STANDARD' END AS duration_sec_times_session_no_tier,
    CASE WHEN session_start_flag >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END AS session_start_flag_tier,
    SUM(CASE WHEN event_type = 'add_to_cart' THEN session_start_flag ELSE 0 END) AS add_to_cart_session_start_flag,
    COUNT(DISTINCT event_type) AS distinct_event_types,
    MAX(event_time) AS last_event_time,
    COUNT(*) FILTER (WHERE event_type = 'purchase') AS purchase_rows,
    GROUPING_ID() AS grouping_id
FROM valid
GROUP BY CUBE (CASE WHEN duration_sec_times_session_no >= 77 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN session_start_flag >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END)
HAVING purchase_rows >= 1
ORDER BY distinct_event_types ASC NULLS LAST;
