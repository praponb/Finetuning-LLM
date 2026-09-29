SELECT
    oi.order_id,
    oi.quantity,
    o.customer_id,
    p.category,
    p.product_name
FROM order_items oi
JOIN orders o ON oi.order_id = o.id
LEFT JOIN products p ON oi.product_id = p.id
WHERE o.status IN ('CANCELLED', 'RETURNED', 'PENDING') AND p.launch_date > '2023-11-01'
ORDER BY category DESC, 2 DESC
