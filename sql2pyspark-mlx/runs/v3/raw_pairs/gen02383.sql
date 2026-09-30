SELECT
    s.id,
    s.customer_id,
    s.country,
    s.browser
FROM sessions s
WHERE s.device IN ('desktop', 'mobile', 'tablet') AND EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id)
ORDER BY s.browser;
