WITH src AS (
    SELECT *
    FROM web_events
    WHERE event_type IN ('add_to_cart', 'click')
),
web_events_stats AS (
    SELECT TO_DATE(event_time) AS event_day, MIN(event_time) AS min_event_time, SORT_ARRAY(COLLECT_SET(event_type)) AS event_type_list
    FROM src
    GROUP BY TO_DATE(event_time)
)
SELECT *
FROM web_events_stats
