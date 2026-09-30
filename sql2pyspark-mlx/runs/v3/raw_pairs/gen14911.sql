SELECT
    o.id,
    o.order_date,
    t.returned_cost,
    t.min_cost
FROM orders o
INNER JOIN (SELECT order_id, SUM(CASE WHEN status = 'RETURNED' THEN cost ELSE 0 END) AS returned_cost, MIN(cost) AS min_cost
    FROM shipments
    GROUP BY order_id) t ON o.id = t.order_id
WHERE t.min_cost > 72
ORDER BY min_cost DESC;
