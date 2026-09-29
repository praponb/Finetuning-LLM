SELECT s.city, i.quantity, LENGTH(s.store_name) AS store_name_length FROM stores s LEFT OUTER JOIN inventory i ON i.store_id = s.id WHERE i.last_restock >= '2025-02-23' ORDER BY s.city DESC
