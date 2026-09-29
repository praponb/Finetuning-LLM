SELECT o.status, MAX(s.cost) - MIN(s.cost) cost_range, MIN(s.delivered_date) first_delivered_date
FROM shipments s
INNER JOIN orders o ON o.id = s.order_id
GROUP BY o.status
ORDER BY first_delivered_date, o.status
