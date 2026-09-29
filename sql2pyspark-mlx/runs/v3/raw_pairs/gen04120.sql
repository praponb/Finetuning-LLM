SELECT
    id,
    carrier,
    shipped_date,
    cost,
    FIRST_VALUE(cost) OVER (PARTITION BY carrier ORDER BY shipped_date, id) AS first_cost
FROM shipments
WHERE order_id IS NULL
