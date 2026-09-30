SELECT carrier, MAX(cost) AS max_cost, MAX(cost) - MIN(cost) AS cost_range
FROM shipments
WHERE order_id IN (7, 35)
GROUP BY carrier
