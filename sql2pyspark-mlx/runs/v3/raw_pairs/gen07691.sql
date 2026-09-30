SELECT o.status, t.avg_unit_price
FROM orders o
INNER JOIN (SELECT order_id, ROUND(AVG(unit_price), 2) avg_unit_price
    FROM order_items
    WHERE unit_price >= 350
    GROUP BY order_id) t ON o.id = t.order_id;
