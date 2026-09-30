SELECT
    o.channel,
    COUNT(*) AS num_rows,
    SUM(e.quantity) AS total_quantity,
    ROUND(AVG(e.unit_price), 2) AS avg_unit_price
FROM order_items e
INNER JOIN orders o ON e.order_id = o.id
GROUP BY ROLLUP (o.channel);
