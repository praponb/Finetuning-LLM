SELECT o.amount, t.unique_statuss
FROM orders o
LEFT OUTER JOIN (SELECT order_id, COUNT(DISTINCT cost) AS unique_statuss
    FROM shipments
    GROUP BY order_id) t ON o.id = t.order_id
WHERE t.unique_statuss > 6;
