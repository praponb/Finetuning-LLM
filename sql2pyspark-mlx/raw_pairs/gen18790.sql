SELECT
    s.city,
    i.store_id,
    i.last_restock,
    i.quantity
FROM stores s
LEFT JOIN inventory i ON i.store_id = s.id
ORDER BY quantity;
