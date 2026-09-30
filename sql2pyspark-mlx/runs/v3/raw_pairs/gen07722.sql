WITH numbered AS (
    SELECT
        id,
        country,
        rating,
        supplier_name,
        RANK() OVER (PARTITION BY country ORDER BY rating DESC) AS row_num
    FROM suppliers
)
SELECT
    id,
    country,
    rating,
    supplier_name
FROM numbered
WHERE row_num <= 3
ORDER BY rating DESC;
