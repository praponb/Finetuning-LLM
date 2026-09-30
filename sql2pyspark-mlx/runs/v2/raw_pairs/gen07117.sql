SELECT
    id,
    status,
    order_id,
    cost,
    shipped_date,
    AVG(cost) OVER (PARTITION BY status, order_id) status_avg_cost,
    RANK() OVER (PARTITION BY order_id ORDER BY shipped_date) rnk
FROM shipments
