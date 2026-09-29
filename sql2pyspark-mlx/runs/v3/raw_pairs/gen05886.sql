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
        DENSE_RANK() OVER (PARTITION BY country ORDER BY rating DESC) AS rnk
    FROM suppliers
) AS t
WHERE rnk <= 2
ORDER BY country
