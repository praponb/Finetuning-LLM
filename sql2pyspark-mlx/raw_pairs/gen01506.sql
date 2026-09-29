SELECT carrier, status, SUM(cost) AS total_cost
FROM shipments
WHERE carrier = 'DHL' AND (delivered_date > '2023-12-01' OR delivered_date >= '2023-07-26')
GROUP BY 1, 2
ORDER BY carrier
