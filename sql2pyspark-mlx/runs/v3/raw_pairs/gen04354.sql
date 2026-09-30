SELECT
    s.id,
    s.browser,
    s.country,
    s.customer_id
FROM sessions s
WHERE s.browser = 'Chrome' AND s.customer_id IN (SELECT id FROM customers WHERE is_active = TRUE)
