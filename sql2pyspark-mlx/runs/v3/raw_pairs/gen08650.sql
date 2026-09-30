SELECT
    id,
    order_id,
    cost,
    shipped_date,
    MIN(cost) OVER (PARTITION BY order_id) AS order_id_min_cost,
    DENSE_RANK() OVER (ORDER BY shipped_date) AS shipped_date_dense_rank
FROM shipments
ORDER BY cost DESC;
