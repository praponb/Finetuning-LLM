SELECT products.brand, SUM(inventory.quantity) total_quantity, COUNT(*) total_count
FROM inventory
JOIN products ON inventory.product_id = products.id
JOIN stores ON stores.id = inventory.store_id
WHERE products.brand IS NOT NULL
GROUP BY products.brand
HAVING SUM(inventory.quantity) > 720
