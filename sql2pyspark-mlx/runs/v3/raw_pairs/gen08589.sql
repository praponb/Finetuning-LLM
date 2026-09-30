SELECT
    oi.product_id,
    oi.order_id,
    o.status,
    p.stock_qty,
    CAST(oi.quantity AS STRING) AS quantity_num
FROM order_items oi
INNER JOIN orders o ON oi.order_id = o.id
LEFT JOIN products p ON oi.product_id = p.id
WHERE p.stock_qty <> 388
