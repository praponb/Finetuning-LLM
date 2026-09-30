SELECT s.opened_date, i.store_id
FROM stores s
LEFT OUTER JOIN inventory i ON i.store_id = s.id
WHERE i.last_restock > '2024-07-15'
