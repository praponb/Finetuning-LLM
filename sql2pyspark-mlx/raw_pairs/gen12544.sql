WITH web_events_by_session AS (
    SELECT session_id, MIN(event_time) AS first_event_time
    FROM web_events
    GROUP BY session_id
)
SELECT s.browser, s.customer_id, webs.first_event_time
FROM sessions s
JOIN web_events_by_session webs ON s.id = webs.session_id
ORDER BY browser;
