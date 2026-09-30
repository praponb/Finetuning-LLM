SELECT last_restock
FROM inventory
INTERSECT
SELECT launch_date
FROM products
WHERE brand IN ('Umbrella', 'Globex');
