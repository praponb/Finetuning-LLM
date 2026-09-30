SELECT
    we.event_time,
    we.duration_sec,
    we.event_type,
    s.browser,
    s.started_at,
    s.device,
    CONCAT(s.browser, '/', s.country) AS browser_country
FROM web_events we
JOIN sessions s ON we.session_id = s.id
WHERE s.customer_id = 9 OR we.session_id IN (7, 17)
