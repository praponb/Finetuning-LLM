SELECT inventory.id, inventory.quantity, inventory.last_restock
FROM inventory
WHERE inventory.store_id NOT IN (SELECT id FROM stores WHERE store_name LIKE '%Riverside%')
ORDER BY inventory.id DESC
LIMIT 10;
