SELECT
    oi.id,
    oi.quantity,
    oi.product_id,
    p.price,
    p.cost,
    p.stock_qty
FROM order_items oi
LEFT JOIN products p ON oi.product_id = p.id
WHERE oi.quantity >= 5
ORDER BY cost DESC;
