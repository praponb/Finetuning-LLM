SELECT DATE_TRUNC('MONTH', st.opened_date) AS opened_month, AVG(s.cost) AS avg_cost
FROM shipments s
INNER JOIN orders o ON s.order_id = o.id
INNER JOIN stores st ON o.store_id = st.id
GROUP BY DATE_TRUNC('MONTH', st.opened_date)
ORDER BY avg_cost
