SELECT st.city, MAX(s.delivered_date) AS last_delivered_date, ROUND(AVG(s.cost), 2) AS avg_cost
FROM shipments s
INNER JOIN orders o ON s.order_id = o.id
INNER JOIN stores st ON o.store_id = st.id
GROUP BY st.city
ORDER BY 3 DESC, city
LIMIT 20
