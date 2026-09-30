SELECT
    s.id,
    s.device,
    s.browser,
    s.customer_id
FROM sessions s
WHERE s.customer_id NOT IN (SELECT id FROM customers WHERE YEAR(signup_date) >= 2025);
