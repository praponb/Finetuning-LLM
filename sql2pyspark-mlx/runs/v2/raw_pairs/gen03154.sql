WITH ranked AS (
    SELECT
        id,
        category,
        cost,
        supplier_id,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY cost DESC, id) AS rnk
    FROM products
)
SELECT *
FROM ranked
WHERE rnk <= 1;
