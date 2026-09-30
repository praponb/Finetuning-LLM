SELECT
    we.id,
    we.page,
    we.event_type,
    s.browser
FROM web_events we
LEFT JOIN sessions s ON s.id = we.session_id
WHERE s.device IN ('tablet', 'mobile', 'desktop') AND s.customer_id IN (7, 27, 35)
ORDER BY event_type, id DESC;
