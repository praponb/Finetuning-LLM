SELECT
    oi.quantity,
    oi.unit_price,
    p.stock_qty,
    o.store_id,
    o.order_date
FROM order_items oi
INNER JOIN products p ON oi.product_id = p.id
INNER JOIN orders o ON oi.order_id = o.id
WHERE oi.product_id IN (23, 28, 29) OR o.discount BETWEEN 0.35 AND 0.36
ORDER BY oi.unit_price ASC
