SELECT
    oi.product_id,
    p.price,
    o.shipping_fee,
    o.store_id
FROM order_items oi
LEFT OUTER JOIN products p ON oi.product_id = p.id
JOIN orders o ON oi.order_id = o.id
ORDER BY store_id DESC, price DESC;
