SELECT s.store_name, o.status
FROM stores s
JOIN orders o ON s.id = o.store_id
