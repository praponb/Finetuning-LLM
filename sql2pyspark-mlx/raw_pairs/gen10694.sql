SELECT
    DATE_TRUNC('MONTH', c.signup_date) AS signup_month,
    c.is_active,
    AVG(o.amount) AS avg_amount,
    COUNT(CASE WHEN o.status = 'RETURNED' THEN 1 END) AS returned_count
FROM orders o
RIGHT JOIN customers c ON o.customer_id = c.id
LEFT JOIN stores s ON o.store_id = s.id
GROUP BY DATE_TRUNC('MONTH', c.signup_date), c.is_active
HAVING AVG(o.amount) >= 1490
