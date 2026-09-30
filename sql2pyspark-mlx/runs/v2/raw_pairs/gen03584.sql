SELECT s.city, COUNT(o.id) AS order_count FROM stores s LEFT JOIN orders o ON s.id = o.store_id WHERE s.region IS NOT NULL GROUP BY s.city
