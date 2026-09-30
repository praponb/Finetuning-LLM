SELECT
    s.status,
    o.order_date,
    o.customer_id,
    o.status AS order_status
FROM shipments s
JOIN orders o ON s.order_id = o.id
WHERE o.order_date < '2025-08-12'
ORDER BY order_status;
