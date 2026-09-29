SELECT s.city, s.sqft, t.total_count FROM stores s LEFT OUTER JOIN (SELECT store_id, COUNT(*) AS total_count FROM store_sales GROUP BY store_id) t ON s.id = t.store_id WHERE t.total_count > 6
