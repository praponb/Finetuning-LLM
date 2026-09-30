SELECT
    p.method,
    o.amount,
    c.email,
    c.is_active
FROM payments p
INNER JOIN orders o ON p.order_id = o.id
INNER JOIN customers c ON o.customer_id = c.id
WHERE YEAR(o.order_date) = 2023 OR c.is_active = TRUE
ORDER BY p.method;
