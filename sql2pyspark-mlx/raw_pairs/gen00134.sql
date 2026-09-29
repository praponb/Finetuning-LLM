SELECT QUARTER(p.launch_date) AS launch_quarter, COUNT(DISTINCT oi.unit_price) AS unique_orders
FROM order_items oi
FULL OUTER JOIN products p ON oi.product_id = p.id
RIGHT OUTER JOIN orders o ON oi.order_id = o.id
WHERE UPPER(p.product_name) LIKE '%OGA%'
GROUP BY QUARTER(p.launch_date)
HAVING unique_orders > 1
ORDER BY launch_quarter
