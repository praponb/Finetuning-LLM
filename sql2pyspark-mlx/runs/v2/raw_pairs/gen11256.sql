SELECT
    oi.id,
    oi.product_id,
    oi.order_id,
    p.brand,
    p.cost,
    p.price
FROM order_items oi
INNER JOIN products p ON p.id = oi.product_id
