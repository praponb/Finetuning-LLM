SELECT
    i.id,
    i.last_restock,
    i.quantity,
    s.city
FROM inventory i
INNER JOIN stores s ON i.store_id = s.id;
