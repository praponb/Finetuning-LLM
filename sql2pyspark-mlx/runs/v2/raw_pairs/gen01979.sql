SELECT
    id,
    carrier,
    delivered_date,
    ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY delivered_date, id) AS rn
FROM shipments
ORDER BY delivered_date
