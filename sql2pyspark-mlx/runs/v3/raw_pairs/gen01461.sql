SELECT MONTH(delivered_date) AS delivered_month, MAX(cost) AS max_cost
FROM shipments
GROUP BY MONTH(delivered_date)
ORDER BY delivered_month DESC
LIMIT 20
