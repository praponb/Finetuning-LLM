SELECT
    id,
    shipped_date,
    cost,
    LAG(cost, 1) OVER (ORDER BY shipped_date, id) prev_cost
FROM shipments
WHERE order_id = 12
