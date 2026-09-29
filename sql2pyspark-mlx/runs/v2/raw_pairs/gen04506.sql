SELECT s.region, MIN(o.order_date) AS first_order_date, ROUND(STDDEV(o.discount), 2) AS stddev_discount
FROM orders o
RIGHT JOIN stores s ON s.id = o.store_id
GROUP BY s.region
HAVING ROUND(STDDEV(o.discount), 2) >= 49
ORDER BY first_order_date DESC
