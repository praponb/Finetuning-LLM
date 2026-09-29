SELECT
    id,
    order_id,
    shipped_date,
    cost,
    LEAD(cost, 2) OVER (PARTITION BY order_id ORDER BY shipped_date, id) AS next_cost
FROM shipments
WHERE cost <= 94
ORDER BY cost
