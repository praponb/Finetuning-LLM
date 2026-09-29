WITH RecentWebEvents AS (
    SELECT *, YEAR(event_time) AS event_time_year
    FROM web_events
    WHERE duration_sec <> 198 OR event_type IN ('search', 'page_view')
),
agg AS (
    SELECT TO_DATE(event_time) AS event_day, session_id, COUNT(DISTINCT event_time_year) AS unique_sessions
    FROM RecentWebEvents
    GROUP BY TO_DATE(event_time), session_id
)
SELECT *
FROM agg
WHERE unique_sessions > 8
ORDER BY event_day, session_id
LIMIT 10
