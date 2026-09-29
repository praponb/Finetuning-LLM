SELECT inventory.id, inventory.last_restock
FROM inventory
WHERE inventory.product_id IN (1, 6, 9, 10) AND inventory.product_id IN (SELECT id FROM products WHERE cost BETWEEN 220 AND 300)
