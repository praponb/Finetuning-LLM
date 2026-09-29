SELECT
    d.channel,
    COUNT(*) AS rows_total,
    AVG(e.quantity) AS avg_quantity
FROM order_items e
INNER JOIN orders d ON e.order_id = d.id
GROUP BY d.channel
ORDER BY avg_quantity DESC;
