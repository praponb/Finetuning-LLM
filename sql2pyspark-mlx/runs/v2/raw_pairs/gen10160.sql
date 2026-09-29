SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY cost, id) rn
    FROM shipments
) r
WHERE rn <= 3;
