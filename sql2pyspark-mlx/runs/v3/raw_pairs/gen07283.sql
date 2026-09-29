SELECT DATE_TRUNC('MONTH', shipped_date) AS shipped_month, status, AVG(cost) AS avg_cost
FROM shipments
WHERE MONTH(delivered_date) IN (6, 8, 9)
GROUP BY DATE_TRUNC('MONTH', shipped_date), status
ORDER BY status DESC, shipped_month
LIMIT 3
