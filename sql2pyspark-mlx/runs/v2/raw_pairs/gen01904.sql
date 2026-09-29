SELECT
    id,
    carrier,
    cost,
    AVG(cost) OVER (PARTITION BY carrier) AS carrier_avg_cost
FROM shipments
WHERE order_id IS NOT NULL
