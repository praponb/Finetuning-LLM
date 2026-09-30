WITH numbered AS (
    SELECT
        id,
        city,
        rating,
        supplier_name,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC, id) row_num
    FROM suppliers
    WHERE city IN ('New York', 'Bangkok', 'Singapore')
)
SELECT
    id,
    city,
    rating,
    supplier_name
FROM numbered
WHERE row_num <= 3
