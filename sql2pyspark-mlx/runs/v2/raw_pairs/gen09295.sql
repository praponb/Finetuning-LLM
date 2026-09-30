WITH FilteredWebEvents AS (
    SELECT *, DATE_FORMAT(event_time, 'yyyy-MM') AS event_time_label
    FROM web_events
    WHERE page = '/checkout' AND page <> '/home'
),
agg AS (
    SELECT event_type, SORT_ARRAY(COLLECT_SET(event_type)) AS event_type_list, AVG(duration_sec) AS avg_duration_sec
    FROM FilteredWebEvents
    GROUP BY event_type
)
SELECT event_type, event_type_list, avg_duration_sec
FROM agg
WHERE avg_duration_sec < 350
ORDER BY event_type
LIMIT 10;
