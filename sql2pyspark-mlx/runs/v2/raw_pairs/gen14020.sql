SELECT
    oi.id,
    oi.order_id,
    o.order_date,
    o.shipping_fee,
    c.age,
    c.is_active
FROM order_items oi
INNER JOIN orders o ON oi.order_id = o.id
INNER JOIN customers c ON o.customer_id = c.id
WHERE oi.product_id = 6
