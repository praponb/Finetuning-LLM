SELECT
    id,
    country,
    rating,
    city
FROM (
    SELECT
        id,
        country,
        rating,
        city,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC, id) AS pos
    FROM suppliers
    WHERE country NOT IN ('US', 'UK')
) AS ranked
WHERE pos <= 2;
