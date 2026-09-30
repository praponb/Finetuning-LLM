SELECT
    we.session_id,
    we.event_time,
    we.page,
    s.device,
    s.country,
    s.browser
FROM web_events we
INNER JOIN sessions s ON we.session_id = s.id;
