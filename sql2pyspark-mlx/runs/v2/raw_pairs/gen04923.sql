SELECT
    oi.id,
    oi.unit_price,
    oi.order_id,
    p.product_name,
    p.stock_qty
FROM order_items oi
INNER JOIN products p ON oi.product_id = p.id
ORDER BY p.stock_qty, p.product_name;
