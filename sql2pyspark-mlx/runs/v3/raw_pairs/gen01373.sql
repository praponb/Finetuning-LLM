SELECT DATE_TRUNC('MONTH', delivered_date) AS delivered_month, MAX(cost) AS max_cost
FROM shipments
GROUP BY DATE_TRUNC('MONTH', delivered_date)
HAVING max_cost < 29
