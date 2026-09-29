SELECT
    we.id,
    we.event_type,
    s.customer_id,
    s.country,
    s.browser
FROM web_events we
INNER JOIN sessions s ON we.session_id = s.id
