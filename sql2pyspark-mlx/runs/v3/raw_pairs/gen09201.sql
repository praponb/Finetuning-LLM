SELECT
    id,
    carrier,
    cost,
    DENSE_RANK() OVER (PARTITION BY carrier ORDER BY cost DESC) AS cost_dense_rank
FROM shipments
ORDER BY carrier DESC, cost;
