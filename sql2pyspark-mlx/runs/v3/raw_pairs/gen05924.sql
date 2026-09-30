SELECT products.category, MAX(inventory.last_restock) AS last_last_restock, COUNT(DISTINCT inventory.quantity) AS unique_stores
FROM inventory
JOIN stores ON inventory.store_id = stores.id
JOIN products ON inventory.product_id = products.id
WHERE inventory.product_id IN (1, 3, 7, 10)
GROUP BY products.category
ORDER BY unique_stores DESC;
