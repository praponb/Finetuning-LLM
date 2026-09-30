SELECT s.cost, o.store_id, CASE WHEN s.status = 'LOST' THEN 1 ELSE 0 END AS is_lost
FROM shipments s
INNER JOIN orders o ON o.id = s.order_id;
