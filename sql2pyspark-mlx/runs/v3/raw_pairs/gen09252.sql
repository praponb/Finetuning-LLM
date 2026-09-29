WITH product_summary AS (
    SELECT product_id, MAX(last_restock) AS latest_last_restock, ROUND(STDDEV(quantity), 2) AS stddev_quantity
    FROM inventory
    WHERE quantity = 192
    GROUP BY product_id
)
SELECT
    products.id,
    products.cost,
    products.product_name,
    product_summary.latest_last_restock,
    product_summary.stddev_quantity
FROM products
INNER JOIN product_summary ON products.id = product_summary.product_id
ORDER BY products.product_name, id DESC
LIMIT 20
