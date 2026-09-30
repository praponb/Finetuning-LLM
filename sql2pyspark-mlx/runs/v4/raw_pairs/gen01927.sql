WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC, id) AS rnk
    FROM suppliers
    WHERE city IS NOT NULL
)
SELECT
    id,
    country,
    rating,
    city
FROM numbered
WHERE rnk <= 5;
