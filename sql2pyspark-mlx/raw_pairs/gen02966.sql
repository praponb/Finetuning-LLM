SELECT launch_date
FROM products
WHERE launch_date IS NOT NULL
EXCEPT
SELECT last_restock
FROM inventory
WHERE last_restock IS NOT NULL
