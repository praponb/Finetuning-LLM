SELECT
    s.city,
    s.region,
    i.last_restock,
    i.store_id
FROM stores s
INNER JOIN inventory i ON i.store_id = s.id
ORDER BY store_id DESC;
