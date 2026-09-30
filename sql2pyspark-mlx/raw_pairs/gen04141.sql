SELECT id, city, rating
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating, id) AS row_num
    FROM suppliers
) AS ranked
WHERE row_num <= 5;
