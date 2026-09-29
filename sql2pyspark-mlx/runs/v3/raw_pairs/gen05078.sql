SELECT
    we.page,
    s.browser,
    s.customer_id,
    c.name,
    c.segment
FROM web_events we
INNER JOIN sessions s ON we.session_id = s.id
LEFT JOIN customers c ON s.customer_id = c.id
WHERE TO_DATE(s.started_at) >= '2024-07-01' AND we.event_time >= '2024-12-20'
