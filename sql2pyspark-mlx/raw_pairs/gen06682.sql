WITH ranked_products AS (
    SELECT
        id,
        category,
        stock_qty,
        RANK() OVER (PARTITION BY category ORDER BY stock_qty) AS row_num
    FROM products
)
SELECT id, category, stock_qty
FROM ranked_products
WHERE row_num <= 3
