WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC, id) AS row_num
    FROM suppliers
    WHERE country = 'JP'
)
SELECT
    id,
    country,
    rating,
    supplier_name
FROM ranked
WHERE row_num <= 5
ORDER BY supplier_name
