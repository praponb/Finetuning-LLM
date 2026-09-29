WITH inventory_by_product AS (
    SELECT product_id, COUNT(quantity) AS quantity_count
    FROM inventory
    GROUP BY product_id
)
SELECT products.launch_date, products.price, inventory_by_product.quantity_count
FROM products
INNER JOIN inventory_by_product ON products.id = inventory_by_product.product_id
WHERE inventory_by_product.quantity_count > 6;
