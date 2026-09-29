SELECT inventory.id, inventory.product_id, inventory.last_restock
FROM inventory
WHERE inventory.product_id IN (SELECT id FROM products WHERE cost IS NULL);
