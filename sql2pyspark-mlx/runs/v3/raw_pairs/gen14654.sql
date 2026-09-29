SELECT *, CAST(cost AS INT) AS cost_int
FROM shipments
WHERE status = 'LOST' AND carrier IN ('FedEx', 'Kerry')
