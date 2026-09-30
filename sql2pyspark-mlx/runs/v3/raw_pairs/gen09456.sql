WITH top_suppliers AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY country ORDER BY rating DESC) AS rn
    FROM suppliers
)
SELECT id, country, rating
FROM top_suppliers
WHERE rn = 1
ORDER BY country
