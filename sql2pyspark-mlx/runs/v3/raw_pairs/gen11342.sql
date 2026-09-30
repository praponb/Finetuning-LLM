SELECT
    products.id,
    products.supplier_id,
    products.product_name,
    t.avg_quantity,
    t.min_last_restock
FROM products
INNER JOIN (SELECT product_id, AVG(quantity) AS avg_quantity, MIN(last_restock) AS min_last_restock
    FROM inventory
    GROUP BY product_id) t ON products.id = t.product_id
ORDER BY avg_quantity;
