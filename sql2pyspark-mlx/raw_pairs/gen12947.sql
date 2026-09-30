WITH ranked_products AS (
    SELECT
        id,
        brand,
        cost,
        product_name,
        ROW_NUMBER() OVER (PARTITION BY brand ORDER BY cost DESC, id) AS rnk
    FROM products
)
SELECT
    id,
    brand,
    cost,
    product_name
FROM ranked_products
WHERE rnk = 1
ORDER BY cost DESC, id DESC
