SELECT id, country, rating
FROM (
    SELECT
        id,
        country,
        rating,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC, id) pos
    FROM suppliers
) r
WHERE pos <= 1
ORDER BY rating DESC
