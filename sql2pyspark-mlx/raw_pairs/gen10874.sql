SELECT id, carrier, delivered_date
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY delivered_date DESC, id) AS pos
    FROM shipments
) AS ranked
WHERE pos <= 3;
