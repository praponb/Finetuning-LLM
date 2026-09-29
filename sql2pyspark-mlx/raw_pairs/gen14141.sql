SELECT orders.channel, YEAR(orders.order_date) AS order_year, SORT_ARRAY(COLLECT_SET(shipments.carrier)) AS carrier_list
FROM shipments
FULL OUTER JOIN orders ON shipments.order_id = orders.id
JOIN customers ON orders.customer_id = customers.id
WHERE MONTH(orders.order_date) IN (1, 6, 9)
GROUP BY orders.channel, YEAR(orders.order_date)
