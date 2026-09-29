SELECT id, status, delivered_date
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY delivered_date DESC, id) AS rn
    FROM shipments
) AS t
WHERE rn <= 5
ORDER BY id
