SELECT
    we.id,
    we.event_type,
    we.page,
    s.country,
    s.browser
FROM web_events we
JOIN sessions s ON we.session_id = s.id
WHERE we.session_id = 26 AND s.device = 'desktop';
