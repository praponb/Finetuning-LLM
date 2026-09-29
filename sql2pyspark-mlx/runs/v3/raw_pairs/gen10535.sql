WITH web_events_by_session AS (
    SELECT session_id, MIN(duration_sec) AS min_duration_sec, COUNT(*) AS web_event_count
    FROM web_events
    GROUP BY session_id
)
SELECT
    s.id,
    s.country,
    s.browser,
    webs.min_duration_sec,
    webs.web_event_count
FROM sessions s
INNER JOIN web_events_by_session webs ON s.id = webs.session_id
ORDER BY s.country
