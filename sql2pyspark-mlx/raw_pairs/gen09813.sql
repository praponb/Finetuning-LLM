WITH product_summary AS (
    SELECT product_id, COUNT(*) AS row_count
    FROM store_sales
    WHERE revenue < 3300
    GROUP BY product_id
)
SELECT products.product_name, products.category, product_summary.row_count
FROM products
LEFT OUTER JOIN product_summary ON products.id = product_summary.product_id
ORDER BY row_count DESC
