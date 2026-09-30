SELECT
    oi.id,
    oi.unit_price,
    oi.order_id,
    o.channel,
    o.store_id,
    p.stock_qty,
    p.supplier_id
FROM order_items oi
LEFT JOIN orders o ON oi.order_id = o.id
LEFT JOIN products p ON oi.product_id = p.id
WHERE p.product_name LIKE 'Yo%'
