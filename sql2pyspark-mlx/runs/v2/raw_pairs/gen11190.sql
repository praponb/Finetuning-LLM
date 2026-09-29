SELECT
    oi.id,
    oi.product_id,
    oi.order_id,
    p.price,
    p.stock_qty,
    p.product_name
FROM order_items oi
LEFT OUTER JOIN products p ON oi.product_id = p.id
WHERE oi.order_id = 33
ORDER BY oi.id ASC, product_id ASC
