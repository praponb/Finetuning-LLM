SELECT
    s.browser,
    s.country,
    c.is_active,
    c.name
FROM sessions s
RIGHT OUTER JOIN customers c ON s.customer_id = c.id
WHERE s.browser = 'Edge' OR s.browser IN ('Safari', 'Chrome', 'Edge')
ORDER BY s.browser, name
