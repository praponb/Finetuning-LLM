SELECT
    id,
    status,
    shipped_date,
    cost,
    FIRST_VALUE(cost) OVER (PARTITION BY status ORDER BY shipped_date DESC, id) AS first_cost
FROM shipments
WHERE delivered_date >= '2024-04-02'
