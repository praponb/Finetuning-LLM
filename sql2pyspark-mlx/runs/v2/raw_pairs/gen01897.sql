SELECT store_id, COUNT(CASE WHEN channel = 'web' THEN 1 END) web_count FROM orders GROUP BY store_id HAVING web_count < 3
