SELECT
    p.amount,
    p.method,
    o.amount AS order_amount,
    o.order_date
FROM payments p
JOIN orders o ON p.order_id = o.id
WHERE o.customer_id IS NULL
ORDER BY o.amount DESC
