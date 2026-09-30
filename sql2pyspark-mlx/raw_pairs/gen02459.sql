SELECT
    id,
    shipped_date,
    cost,
    status,
    SUM(cost) OVER (PARTITION BY order_id ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_cost,
    SUM(cost) OVER (PARTITION BY status ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_cost
FROM shipments
