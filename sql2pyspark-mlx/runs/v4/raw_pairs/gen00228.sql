SELECT id, carrier, cost
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY cost, id) AS pos
    FROM shipments
    WHERE status = 'IN_TRANSIT'
) AS ranked
WHERE pos <= 2;
