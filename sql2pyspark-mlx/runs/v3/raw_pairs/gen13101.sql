SELECT
    oi.id,
    oi.product_id,
    oi.unit_price,
    o.order_date,
    o.amount,
    p.cost,
    p.cost + p.price AS margin
FROM order_items oi
LEFT JOIN orders o ON oi.order_id = o.id
INNER JOIN products p ON p.id = oi.product_id
ORDER BY cost DESC;
