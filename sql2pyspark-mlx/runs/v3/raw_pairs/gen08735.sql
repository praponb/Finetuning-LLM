SELECT
    s.id,
    s.browser,
    s.device,
    s.started_at
FROM sessions s
WHERE s.customer_id IN (SELECT id FROM customers WHERE is_active = FALSE)
