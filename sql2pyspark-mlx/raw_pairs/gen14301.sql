SELECT
    oi.product_id,
    oi.order_id,
    o.store_id,
    o.order_date,
    p.brand
FROM order_items oi
INNER JOIN orders o ON oi.order_id = o.id
INNER JOIN products p ON oi.product_id = p.id;
