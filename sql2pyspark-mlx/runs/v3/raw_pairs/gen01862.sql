WITH ranked_suppliers AS (
    SELECT
        id,
        country,
        rating,
        supplier_name,
        DENSE_RANK() OVER (PARTITION BY country ORDER BY rating DESC) AS rn
    FROM suppliers
)
SELECT
    id,
    country,
    rating,
    supplier_name
FROM ranked_suppliers
WHERE rn <= 3;
