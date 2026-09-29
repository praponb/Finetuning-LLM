SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating, id) AS row_num
    FROM suppliers
) AS r
WHERE row_num = 1
