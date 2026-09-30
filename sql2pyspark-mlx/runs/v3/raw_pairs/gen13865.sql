SELECT
    s.browser,
    s.started_at,
    s.country,
    we.duration_sec,
    we.session_id
FROM sessions s
LEFT JOIN web_events we ON we.session_id = s.id
WHERE we.session_id IN (8, 40) AND we.event_time < '2024-01-01'
ORDER BY we.session_id, s.country DESC;
