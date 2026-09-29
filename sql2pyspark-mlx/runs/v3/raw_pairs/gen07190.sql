WITH src AS (
    SELECT *, TO_DATE(event_time) AS event_time_date
    FROM web_events
    WHERE page IN ('/home', '/cart', '/search') AND event_type IN ('click', 'purchase')
),
web_events_stats AS (
    SELECT
        CASE WHEN event_type IN ('purchase', 'add_to_cart') THEN 'group_a' ELSE 'other' END AS event_type_group,
        page,
        SUM(CASE WHEN page = '/home' THEN duration_sec ELSE 0 END) AS home_duration_sec,
        MAX(event_time) AS latest_event_time
    FROM src
    GROUP BY CASE WHEN event_type IN ('purchase', 'add_to_cart') THEN 'group_a' ELSE 'other' END, page
)
SELECT event_type_group, page, latest_event_time
FROM web_events_stats
WHERE home_duration_sec >= 26
ORDER BY event_type_group, page DESC;
