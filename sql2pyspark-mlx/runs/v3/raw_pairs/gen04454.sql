SELECT
    id,
    carrier,
    cost,
    MAX(cost) OVER (PARTITION BY carrier) AS carrier_max_cost
FROM shipments
WHERE status IN ('DELIVERED', 'LOST');
