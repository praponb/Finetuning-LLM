SELECT
    oi.quantity,
    p.price,
    p.category,
    o.channel
FROM order_items oi
LEFT JOIN products p ON oi.product_id = p.id
FULL OUTER JOIN orders o ON oi.order_id = o.id
WHERE p.launch_date BETWEEN '2023-03-17' AND '2025-09-01';
