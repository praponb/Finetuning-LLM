WITH inventory_by_product AS (
    SELECT product_id, MAX(last_restock) AS latest_last_restock
    FROM inventory
    GROUP BY product_id
)
SELECT
    products.id,
    products.brand,
    products.launch_date,
    inventory_by_product.latest_last_restock
FROM products
LEFT JOIN inventory_by_product ON products.id = inventory_by_product.product_id
ORDER BY brand
