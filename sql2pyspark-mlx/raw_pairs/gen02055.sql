SELECT store_name, opened_date FROM stores WHERE region != 'Central' UNION SELECT store_name, opened_date FROM stores WHERE city IN ('Singapore', 'Tokyo') ORDER BY store_name
