SELECT
    sessions.id,
    sessions.browser,
    sessions.device,
    sessions.country
FROM sessions
WHERE HOUR(sessions.started_at) BETWEEN 11 AND 17 AND sessions.id IN (SELECT session_id FROM web_events WHERE TO_DATE(event_time) < '2025-09-06');
