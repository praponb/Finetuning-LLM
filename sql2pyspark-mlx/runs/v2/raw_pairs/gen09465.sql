WITH product_summary AS (
    SELECT product_id, SUM(quantity) AS total_quantity, MAX(quantity) - MIN(quantity) AS quantity_range
    FROM inventory
    WHERE last_restock > '2023-11-11'
    GROUP BY product_id
)
SELECT products.brand, product_summary.total_quantity, product_summary.quantity_range
FROM products
INNER JOIN product_summary ON products.id = product_summary.product_id
