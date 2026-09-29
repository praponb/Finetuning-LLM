SELECT orders.status, orders.channel, MIN(shipments.delivered_date) AS first_delivered_date
FROM shipments
FULL OUTER JOIN orders ON shipments.order_id = orders.id
GROUP BY orders.status, orders.channel
ORDER BY 1 DESC, first_delivered_date
