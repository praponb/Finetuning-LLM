SELECT
    oi.quantity,
    p.stock_qty,
    p.price,
    o.discount,
    LEAST(oi.quantity, oi.unit_price) AS least_quantity_unit_price
FROM order_items oi
LEFT JOIN products p ON p.id = oi.product_id
INNER JOIN orders o ON oi.order_id = o.id
WHERE NOT (oi.order_id = 16 AND oi.quantity <= 3)
ORDER BY price DESC;
