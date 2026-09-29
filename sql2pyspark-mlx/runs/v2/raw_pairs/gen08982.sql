SELECT o.status, t.avg_unit_price
FROM orders o
JOIN (SELECT order_id, AVG(unit_price) AS avg_unit_price
    FROM order_items
    GROUP BY order_id) t ON o.id = t.order_id
WHERE t.avg_unit_price >= 370;
