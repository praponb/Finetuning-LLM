SELECT stores.city, COUNT(*) AS total_count, SUM(inventory.quantity) AS total_quantity
FROM inventory
INNER JOIN stores ON inventory.store_id = stores.id
WHERE stores.sqft <= 4704
GROUP BY stores.city
HAVING COUNT(*) >= 2;
