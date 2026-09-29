SELECT
    oi.unit_price,
    oi.order_id,
    p.stock_qty,
    p.brand,
    o.amount
FROM order_items oi
LEFT JOIN products p ON oi.product_id = p.id
INNER JOIN orders o ON o.id = oi.order_id
ORDER BY order_id DESC, p.stock_qty;
