SELECT i.id, i.last_restock
FROM inventory i
WHERE i.product_id NOT IN (SELECT id FROM products WHERE MONTH(launch_date) IN (3, 6, 8))
