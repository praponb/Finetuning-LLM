SELECT
    oi.id,
    oi.product_id,
    oi.unit_price,
    p.launch_date,
    p.stock_qty
FROM order_items oi
LEFT JOIN products p ON p.id = oi.product_id;
