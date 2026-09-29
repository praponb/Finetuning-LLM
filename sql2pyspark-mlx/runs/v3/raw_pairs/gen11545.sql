SELECT
    we.id,
    we.duration_sec,
    we.event_time,
    s.browser,
    s.country,
    c.credit_limit,
    c.segment
FROM web_events we
LEFT JOIN sessions s ON we.session_id = s.id
JOIN customers c ON c.id = s.customer_id
WHERE we.page <> '/cart' AND we.page IS NOT NULL;
