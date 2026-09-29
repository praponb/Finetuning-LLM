SELECT s.id, s.city, COUNT(o.id) AS order_count FROM stores s LEFT JOIN orders o ON o.store_id = s.id GROUP BY s.id, s.city ORDER BY order_count DESC, id, city LIMIT 20
