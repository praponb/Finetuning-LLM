WITH valid_web_events AS (
    SELECT *, CASE WHEN event_type IN ('add_to_cart', 'click') THEN 'Group A' ELSE 'Other' END AS event_type_group
    FROM web_events
    WHERE HOUR(event_time) BETWEEN 11 AND 17 AND page = '/search'
),
summary AS (
    SELECT TO_DATE(event_time) AS event_day, SUM(duration_sec) AS total_duration_sec
    FROM valid_web_events
    GROUP BY TO_DATE(event_time)
)
SELECT event_day, total_duration_sec
FROM summary
WHERE total_duration_sec < 1300;
