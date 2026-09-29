SELECT
    oi.id,
    oi.product_id,
    p.category,
    p.price,
    o.customer_id,
    o.discount - o.amount AS discount_minus_amount
FROM order_items oi
LEFT JOIN products p ON oi.product_id = p.id
RIGHT JOIN orders o ON oi.order_id = o.id
WHERE p.cost >= 480
