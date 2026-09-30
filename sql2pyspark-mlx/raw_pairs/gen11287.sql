WITH numbered AS (
    SELECT
        id,
        city,
        rating,
        country,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating, id) AS row_num
    FROM suppliers
    WHERE country != 'US'
)
SELECT
    id,
    city,
    rating,
    country
FROM numbered
WHERE row_num = 1
ORDER BY rating DESC;
