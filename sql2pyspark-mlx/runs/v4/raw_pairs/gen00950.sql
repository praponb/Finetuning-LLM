SELECT
    we.event_type,
    s.country,
    s.browser,
    c.name
FROM web_events we
LEFT JOIN sessions s ON we.session_id = s.id
INNER JOIN customers c ON c.id = s.customer_id
WHERE s.country = 'DE' OR s.device IN ('tablet', 'desktop', 'mobile')
