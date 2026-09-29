SELECT
    oi.id,
    oi.order_id,
    p.product_name,
    p.price,
    o.customer_id,
    o.store_id
FROM order_items oi
LEFT JOIN products p ON oi.product_id = p.id
LEFT JOIN orders o ON o.id = oi.order_id
WHERE oi.product_id IN (5, 17, 40) AND o.shipping_fee < 34;
