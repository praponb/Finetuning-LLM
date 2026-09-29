SELECT
    oi.id,
    oi.unit_price,
    p.category,
    p.price,
    o.amount,
    o.channel
FROM order_items oi
LEFT OUTER JOIN products p ON oi.product_id = p.id
FULL OUTER JOIN orders o ON oi.order_id = o.id
WHERE o.amount BETWEEN 990 AND 1150 AND oi.quantity BETWEEN 3 AND 7
ORDER BY oi.id
