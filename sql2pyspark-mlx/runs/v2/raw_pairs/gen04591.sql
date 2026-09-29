SELECT
    oi.id,
    oi.product_id,
    oi.unit_price,
    p.price,
    o.channel,
    o.customer_id
FROM order_items oi
RIGHT JOIN products p ON p.id = oi.product_id
INNER JOIN orders o ON oi.order_id = o.id;
