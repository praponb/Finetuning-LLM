WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        event_type,
        page,
        event_time,
        duration_sec,
        CASE WHEN duration_sec >= 407 THEN 'High' WHEN duration_sec >= 202 THEN 'Medium' ELSE 'Low' END AS duration_sec_tier,
        COALESCE(page, 'UNKNOWN') AS resolved_page
    FROM web_events
    WHERE page IS NOT DISTINCT FROM '/search'
      AND duration_sec >= 438
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        HOUR(event_time) AS event_time_hour,
        LOWER(page) AS page_lower
    FROM filtered
    WHERE event_time > DATE '2023-03-25'
      AND event_time >= '2024-08-02'
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM base
),
sessions_web_events AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) AS prev_event_time
    FROM sessions_web_events
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        event_time,
        duration_sec,
        duration_sec_tier,
        resolved_page,
        event_time_hour,
        page_lower,
        session_id,
        session_no,
        LAST_VALUE(event_time_hour, TRUE) OVER (ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_time_hour,
        COUNT(event_time_hour) OVER (PARTITION BY event_type) AS event_type_count_event_time_hour,
        NTILE(4) OVER (ORDER BY event_time_hour, id) AS event_time_hour_bucket
    FROM islands
)
-- Aggregate with conditional metrics
SELECT
    session_id,
    BIT_AND(duration_sec) AS duration_sec_bit_and,
    ROUND(AVG(duration_sec), 2) AS avg_duration_sec
FROM scored
GROUP BY session_id
HAVING BIT_AND(duration_sec) > 1
ORDER BY avg_duration_sec DESC NULLS FIRST;
