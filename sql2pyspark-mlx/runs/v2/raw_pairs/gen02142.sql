SELECT
    oi.id,
    oi.unit_price,
    oi.order_id,
    p.stock_qty
FROM order_items oi
INNER JOIN products p ON p.id = oi.product_id
ORDER BY order_id, stock_qty DESC;
