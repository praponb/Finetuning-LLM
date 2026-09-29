WITH numbered AS (
    SELECT
        id,
        city,
        rating,
        country,
        DENSE_RANK() OVER (PARTITION BY city ORDER BY rating DESC) AS rnk
    FROM suppliers
)
SELECT
    id,
    city,
    rating,
    country
FROM numbered
WHERE rnk <= 3
