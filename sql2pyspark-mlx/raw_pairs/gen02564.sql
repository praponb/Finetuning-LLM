SELECT
    i.id,
    i.last_restock,
    s.store_name,
    s.city
FROM inventory i
LEFT JOIN stores s ON i.store_id = s.id
WHERE s.region <> 'Central'
ORDER BY i.last_restock DESC, id ASC
LIMIT 3;
