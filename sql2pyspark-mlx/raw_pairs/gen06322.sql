SELECT
    c.credit_limit,
    c.name,
    c.email,
    s.customer_id,
    s.browser,
    CONCAT_WS('-', s.device, s.browser) AS device_browser_key
FROM customers c
LEFT OUTER JOIN sessions s ON s.customer_id = c.id
WHERE s.device = 'tablet' OR c.is_active
