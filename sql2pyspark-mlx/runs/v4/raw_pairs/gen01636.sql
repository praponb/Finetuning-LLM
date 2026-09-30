SELECT o.customer_id, MIN(s.cost) AS min_cost, ROUND(STDDEV(s.cost), 2) AS stddev_cost
FROM shipments s
RIGHT JOIN orders o ON s.order_id = o.id
WHERE s.status = 'IN_TRANSIT'
GROUP BY o.customer_id
ORDER BY 2 DESC;
