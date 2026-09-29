SELECT
    s.customer_id,
    c.segment,
    c.name,
    c.is_active
FROM sessions s
RIGHT OUTER JOIN customers c ON s.customer_id = c.id
WHERE YEAR(c.signup_date) = 2025 AND s.device IN ('mobile', 'tablet')
ORDER BY c.name;
