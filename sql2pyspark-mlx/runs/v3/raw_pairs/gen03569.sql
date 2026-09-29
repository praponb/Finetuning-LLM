WITH ranked_suppliers AS (
    SELECT
        id,
        country,
        rating,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating, id) AS row_num
    FROM suppliers
)
SELECT *
FROM ranked_suppliers
WHERE row_num <= 5
ORDER BY country, rating DESC;
