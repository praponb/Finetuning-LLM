SELECT id, carrier, cost
FROM (
    SELECT
        id,
        carrier,
        cost,
        ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY cost DESC, id) AS pos
    FROM shipments
) AS t
WHERE pos <= 3
ORDER BY cost ASC
