SELECT inventory.id, inventory.product_id
FROM inventory
WHERE inventory.quantity BETWEEN 99 AND 213 AND inventory.store_id NOT IN (SELECT id FROM stores WHERE opened_date > '2023-05-01')
ORDER BY inventory.product_id DESC
