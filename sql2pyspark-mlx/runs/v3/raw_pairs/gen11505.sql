WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC, id) AS row_num
    FROM suppliers
)
SELECT id, country, rating
FROM ranked
WHERE row_num = 1
ORDER BY country;
