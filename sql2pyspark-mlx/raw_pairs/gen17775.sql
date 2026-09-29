SELECT
    oi.id,
    oi.unit_price,
    oi.product_id,
    oi.order_id,
    p.launch_date,
    p.cost
FROM order_items oi
JOIN products p ON oi.product_id = p.id;
