SELECT
    id,
    country,
    rating,
    city
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC, id) AS pos
    FROM suppliers
) AS t
WHERE pos <= 5
ORDER BY id DESC, country ASC;
