SELECT
    id,
    order_id,
    carrier,
    status,
    NTILE(5) OVER (PARTITION BY carrier ORDER BY cost DESC, id) AS cost_bucket,
    DENSE_RANK() OVER (PARTITION BY carrier, status ORDER BY cost DESC NULLS FIRST) AS cost_dense_rank
FROM shipments
