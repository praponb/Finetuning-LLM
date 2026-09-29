SELECT MIN(cost) AS min_cost, SUM(CASE WHEN carrier = 'DHL' THEN cost ELSE 0 END) AS dhl_cost
FROM shipments
WHERE shipped_date >= '2023-12-17' AND (delivered_date > '2023-09-01' OR cost >= 92)
ORDER BY dhl_cost DESC
