SELECT
    id,
    cost,
    status,
    shipped_date
FROM shipments
WHERE cost >= (SELECT MAX(cost) FROM shipments)
ORDER BY cost DESC, status DESC, id
LIMIT 10
