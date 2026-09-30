SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating, id) AS rn
    FROM suppliers
) AS r
WHERE rn <= 3
ORDER BY country ASC
