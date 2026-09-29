SELECT
    i.id,
    i.last_restock,
    i.store_id,
    s.store_name,
    s.sqft
FROM inventory i
LEFT JOIN stores s ON i.store_id = s.id;
