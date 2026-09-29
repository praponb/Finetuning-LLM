SELECT
    id,
    status,
    delivered_date,
    cost
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY delivered_date DESC, id) AS row_num
    FROM shipments
) AS ranked
WHERE row_num = 1
ORDER BY 2 DESC, 1 DESC;
