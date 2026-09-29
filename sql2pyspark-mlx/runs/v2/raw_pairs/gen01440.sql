SELECT
    id,
    cost,
    shipped_date,
    carrier
FROM shipments
WHERE cost > (SELECT MAX(cost) FROM shipments)
