SELECT orders.customer_id, MAX(shipments.delivered_date) AS last_delivered_date, AVG(shipments.cost) AS avg_cost
FROM shipments
JOIN orders ON shipments.order_id = orders.id
WHERE shipments.delivered_date BETWEEN '2023-07-15' AND '2024-05-01'
GROUP BY orders.customer_id
ORDER BY last_delivered_date DESC;
