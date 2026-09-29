SELECT id, status, delivered_date
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY delivered_date DESC, id) AS row_num
    FROM shipments
    WHERE delivered_date BETWEEN '2023-08-11' AND '2023-11-23'
) AS t
WHERE row_num = 1;
