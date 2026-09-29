SELECT
    i.id,
    i.store_id,
    i.quantity,
    s.store_name
FROM inventory i
RIGHT JOIN stores s ON i.store_id = s.id
WHERE i.quantity = 193
