SELECT
    oi.product_id,
    oi.order_id,
    p.category,
    p.stock_qty,
    o.customer_id,
    o.shipping_fee
FROM order_items oi
LEFT JOIN products p ON p.id = oi.product_id
LEFT JOIN orders o ON oi.order_id = o.id
WHERE p.launch_date <= '2024-03-01';
