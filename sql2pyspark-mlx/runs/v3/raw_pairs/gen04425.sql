WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC, id) AS row_num
    FROM suppliers
)
SELECT
    id,
    country,
    rating,
    supplier_name
FROM numbered
WHERE row_num = 1
