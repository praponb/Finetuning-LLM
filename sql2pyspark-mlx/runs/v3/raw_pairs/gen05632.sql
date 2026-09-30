WITH ranked_suppliers AS (
    SELECT
        id,
        country,
        rating,
        city,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating, id) AS rnk
    FROM suppliers
)
SELECT
    id,
    country,
    rating,
    city
FROM ranked_suppliers
WHERE rnk <= 3
