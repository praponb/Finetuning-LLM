SELECT shipped_date, delivered_date, CASE WHEN cost >= 92 THEN 'High' WHEN cost >= 41 THEN 'Medium' ELSE 'Low' END AS cost_tier
FROM shipments
WHERE order_id = 35 AND (cost BETWEEN 79 AND 96 OR cost > 56)
ORDER BY delivered_date
