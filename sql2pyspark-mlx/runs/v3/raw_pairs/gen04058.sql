SELECT
    we.id,
    we.duration_sec,
    we.event_type,
    s.country,
    s.started_at,
    s.browser
FROM web_events we
INNER JOIN sessions s ON we.session_id = s.id
WHERE we.page IN ('/home', '/product', '/search') AND s.country IS NULL;
