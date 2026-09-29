SELECT
    s.id,
    s.device,
    s.customer_id,
    s.browser
FROM sessions s
WHERE s.customer_id IS NULL AND NOT EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id AND we.page IN ('/search', '/product', '/checkout'))
ORDER BY customer_id;
