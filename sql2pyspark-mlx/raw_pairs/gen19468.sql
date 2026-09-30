SELECT
    we.id,
    we.event_time,
    we.duration_sec,
    we.page,
    s.customer_id,
    s.browser,
    s.device
FROM web_events we
FULL OUTER JOIN sessions s ON s.id = we.session_id
