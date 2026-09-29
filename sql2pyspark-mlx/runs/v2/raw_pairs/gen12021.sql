SELECT
    oi.id,
    oi.unit_price,
    o.amount,
    o.channel,
    p.cost,
    p.price
FROM order_items oi
LEFT OUTER JOIN orders o ON oi.order_id = o.id
JOIN products p ON oi.product_id = p.id
WHERE oi.unit_price < 220
ORDER BY oi.id DESC
LIMIT 10
