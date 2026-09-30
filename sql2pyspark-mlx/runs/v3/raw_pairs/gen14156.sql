SELECT stores.city, MIN(shipments.cost) AS min_cost, COUNT(DISTINCT shipments.cost) AS unique_statuss
FROM shipments
INNER JOIN orders ON shipments.order_id = orders.id
LEFT OUTER JOIN stores ON stores.id = orders.store_id
WHERE orders.shipping_fee <= 17
GROUP BY stores.city
ORDER BY stores.city DESC
LIMIT 10
