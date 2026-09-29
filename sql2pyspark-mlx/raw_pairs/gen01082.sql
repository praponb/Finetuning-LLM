SELECT
    id,
    order_id,
    status,
    cost,
    DENSE_RANK() OVER (PARTITION BY order_id, status ORDER BY cost DESC) AS dense_rnk
FROM shipments
