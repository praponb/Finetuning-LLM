SELECT
    id,
    status,
    cost,
    NTILE(3) OVER (PARTITION BY status ORDER BY cost, id) AS quartile
FROM shipments
WHERE carrier = 'FedEx';
