SELECT
    oi.id,
    oi.unit_price,
    oi.product_id,
    p.supplier_id,
    p.launch_date,
    o.amount,
    o.shipping_fee
FROM order_items oi
INNER JOIN products p ON p.id = oi.product_id
INNER JOIN orders o ON o.id = oi.order_id
WHERE p.stock_qty BETWEEN 120 AND 254;
