WITH ranked_products AS (
    SELECT
        id,
        category,
        stock_qty,
        product_name,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY stock_qty, id) AS row_num
    FROM products
    WHERE supplier_id IS NOT NULL
)
SELECT
    id,
    category,
    stock_qty,
    product_name
FROM ranked_products
WHERE row_num <= 3
ORDER BY stock_qty DESC;
