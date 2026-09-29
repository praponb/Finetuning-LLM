SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY cost DESC, id) row_num
    FROM shipments
    WHERE cost >= 95
) ranked
WHERE row_num <= 3
ORDER BY status
