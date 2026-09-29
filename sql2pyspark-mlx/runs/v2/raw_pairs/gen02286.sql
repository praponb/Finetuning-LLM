SELECT products.supplier_id, products.category, MIN(inventory.last_restock) first_last_restock
FROM inventory
INNER JOIN products ON inventory.product_id = products.id
LEFT JOIN stores ON inventory.store_id = stores.id
WHERE MONTH(inventory.last_restock) IN (3, 8, 9)
GROUP BY products.supplier_id, products.category
