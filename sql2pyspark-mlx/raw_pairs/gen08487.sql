SELECT status, MAX(cost) AS max_cost
FROM shipments
WHERE carrier IN ('UPS', 'DHL', 'FedEx') AND (status NOT IN ('DELIVERED', 'RETURNED') OR MONTH(delivered_date) IN (2, 4, 10))
GROUP BY status
ORDER BY status DESC
