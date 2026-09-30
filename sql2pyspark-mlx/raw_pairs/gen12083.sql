SELECT YEAR(o.order_date) order_year, ROUND(STDDEV(s.cost), 2) stddev_cost
FROM shipments s
FULL OUTER JOIN orders o ON s.order_id = o.id
FULL OUTER JOIN customers c ON c.id = o.customer_id
WHERE c.segment = 'Corporate'
GROUP BY YEAR(o.order_date)
ORDER BY order_year DESC, stddev_cost;
