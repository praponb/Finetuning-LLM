SELECT o.order_date, t.unique_orders, t.flash_cost
FROM orders o
JOIN (SELECT order_id, COUNT(DISTINCT cost) AS unique_orders, SUM(CASE WHEN carrier = 'Flash' THEN cost ELSE 0 END) AS flash_cost
    FROM shipments
    GROUP BY order_id) t ON o.id = t.order_id;
