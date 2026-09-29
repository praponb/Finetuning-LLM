SELECT
    sessions.started_at,
    sessions.country,
    sessions.browser,
    web_events.event_type,
    web_events.page,
    web_events.event_time
FROM sessions
LEFT JOIN web_events ON web_events.session_id = sessions.id
WHERE HOUR(sessions.started_at) BETWEEN 7 AND 10
ORDER BY country, 4 DESC;
