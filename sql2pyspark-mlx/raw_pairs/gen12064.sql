WITH with_prev AS (
    SELECT *, LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) AS prev_event_time
    FROM web_events
),
with_gaps AS (
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL '60' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM with_prev
),
sessions_web_events AS (
    SELECT
        id,
        event_type,
        page,
        event_time,
        SUM(new_group_flag) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
)
SELECT
    id,
    event_type,
    event_time,
    island_id,
    CASE WHEN event_type RLIKE '^(click|add_to_cart)$' THEN 'EXTENDED' WHEN event_type RLIKE '^(page_view|purchase)$' THEN 'CORE' ELSE 'GROUP_A' END AS event_type_zone
FROM sessions_web_events
