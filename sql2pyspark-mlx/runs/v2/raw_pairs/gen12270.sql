SELECT
    s.id,
    s.country,
    s.device,
    s.browser,
    c.credit_limit
FROM sessions s
JOIN customers c ON s.customer_id = c.id
WHERE c.country IN ('TH', 'SG', 'DE') AND s.customer_id IS NULL
