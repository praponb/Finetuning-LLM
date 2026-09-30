SELECT s.id, s.store_name, s.region, t.last_last_restock FROM stores s INNER JOIN (SELECT store_id, MAX(last_restock) AS last_last_restock FROM inventory GROUP BY store_id) t ON s.id = t.store_id
