WITH valid AS (
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), event_time) AS days_since_event_time,
        COALESCE(page, 'OTHER') AS page_filled
    FROM web_events
    WHERE event_time >= '2023-11-01'
),
gaps AS (
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) > INTERVAL 90 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM valid
),
sessions_web_events AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
with_gaps AS (
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM sessions_web_events
),
sessions_web_events_2 AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
remaining_keys AS (
    SELECT page, id
    FROM sessions_web_events_2
    EXCEPT
    SELECT page, id
    FROM web_events
    WHERE duration_sec BETWEEN 235 AND 281
),
verified AS (
    SELECT s.*
    FROM sessions_web_events_2 s
    INNER JOIN remaining_keys k ON s.page = k.page AND s.id = k.id
)
SELECT
    id,
    event_type,
    page,
    duration_sec,
    page_filled,
    session_start_flag,
    new_group_flag,
    burst_id
FROM verified
ORDER BY id, event_type
