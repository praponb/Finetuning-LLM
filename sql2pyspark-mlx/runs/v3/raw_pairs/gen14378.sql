SELECT id, city, rating
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating, id) AS pos
    FROM suppliers
) AS r
WHERE pos <= 3
ORDER BY rating DESC
