SELECT s.city, COUNT(*) AS total_count, MAX(o.amount) AS max_amount FROM orders o INNER JOIN stores s ON o.store_id = s.id GROUP BY s.city
