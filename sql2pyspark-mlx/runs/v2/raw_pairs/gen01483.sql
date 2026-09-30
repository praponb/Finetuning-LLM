SELECT i.quantity, i.last_restock, s.city
FROM inventory i
INNER JOIN stores s ON i.store_id = s.id
WHERE s.city = 'Singapore' AND s.store_name LIKE 'O%'
