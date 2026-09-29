SELECT
    o.id,
    o.channel,
    o.store_id,
    t.avg_quantity
FROM orders o
JOIN (SELECT order_id, AVG(quantity) AS avg_quantity
    FROM order_items
    GROUP BY order_id) t ON o.id = t.order_id
WHERE t.avg_quantity >= 7.2
