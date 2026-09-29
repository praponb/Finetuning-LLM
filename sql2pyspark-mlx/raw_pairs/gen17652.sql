SELECT
    id,
    status,
    delivered_date,
    cost,
    carrier,
    LEAD(cost, 1) OVER (PARTITION BY status ORDER BY delivered_date, id) AS next_cost,
    DENSE_RANK() OVER (PARTITION BY carrier ORDER BY delivered_date DESC) AS delivered_date_dense_rank
FROM shipments
WHERE order_id IS NULL;
