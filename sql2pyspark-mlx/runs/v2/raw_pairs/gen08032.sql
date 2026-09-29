SELECT
    id,
    order_id,
    shipped_date,
    cost,
    cost - LAG(cost, 1) OVER (PARTITION BY order_id ORDER BY shipped_date, id) cost_change
FROM shipments;
