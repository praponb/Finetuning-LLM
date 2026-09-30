SELECT
    o.channel,
    o.status,
    SUM(oi.unit_price * oi.quantity) AS revenue,
    COUNT(DISTINCT oi.unit_price) AS unique_products
FROM order_items oi
INNER JOIN orders o ON oi.order_id = o.id
WHERE o.discount < 0.18
GROUP BY o.channel, o.status
ORDER BY revenue ASC;
