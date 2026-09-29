SELECT capacity, city
FROM warehouses
WHERE city NOT IN ('Singapore', 'Kuala Lumpur')
EXCEPT
SELECT id AS capacity, category AS city
FROM skus
