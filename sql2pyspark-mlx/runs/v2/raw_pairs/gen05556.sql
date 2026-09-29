WITH ranked_products AS (
    SELECT
        id,
        category,
        cost,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY cost, id) AS row_num
    FROM products
)
SELECT id, category, cost
FROM ranked_products
WHERE row_num <= 3
ORDER BY id;
