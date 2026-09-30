SELECT
    a.id,
    a.order_id,
    a.product_id,
    a.unit_price,
    p.product_name
FROM order_items a
LEFT JOIN products p
    ON a.product_id <=> p.id
