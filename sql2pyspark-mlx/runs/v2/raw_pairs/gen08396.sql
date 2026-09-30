SELECT
    oi.unit_price,
    o.channel,
    p.supplier_id,
    p.price
FROM order_items oi
RIGHT JOIN orders o ON oi.order_id = o.id
JOIN products p ON oi.product_id = p.id
WHERE o.customer_id IS NULL;
