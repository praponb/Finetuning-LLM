SELECT last_restock
FROM inventory
WHERE quantity < 172
INTERSECT
SELECT opened_date
FROM stores
WHERE store_name LIKE 'Riv%';
