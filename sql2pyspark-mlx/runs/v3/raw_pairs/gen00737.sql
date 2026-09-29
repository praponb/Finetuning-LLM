SELECT
    p.id,
    p.method,
    p.paid_at,
    p.order_id
FROM payments p
WHERE p.order_id NOT IN (SELECT id FROM orders WHERE YEAR(order_date) >= 2024)
