SELECT
    oi.id,
    oi.product_id,
    oi.unit_price,
    o.store_id,
    p.brand
FROM order_items oi
INNER JOIN orders o ON oi.order_id = o.id
INNER JOIN products p ON oi.product_id = p.id
WHERE ARRAY_CONTAINS(p.tags, 'bestseller') AND o.shipping_fee > 22
ORDER BY oi.id
