SELECT
    s.id,
    s.order_id,
    o.status,
    c.signup_date
FROM shipments s
INNER JOIN orders o ON s.order_id = o.id
LEFT OUTER JOIN customers c ON o.customer_id = c.id
WHERE YEAR(s.delivered_date) >= 2025
