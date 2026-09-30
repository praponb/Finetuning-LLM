SELECT
    id,
    status,
    delivered_date,
    cost,
    order_id,
    shipped_date,
    LEAD(cost, 1) OVER (PARTITION BY status ORDER BY delivered_date, id) next_cost,
    DENSE_RANK() OVER (PARTITION BY order_id ORDER BY shipped_date) dense_rnk
FROM shipments;
