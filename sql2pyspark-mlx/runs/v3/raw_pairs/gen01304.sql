SELECT id, opened_date, store_name FROM stores WHERE city IN ('London', 'Chiang Mai', 'New York') UNION SELECT id, opened_date, store_name FROM stores WHERE city = 'New York' ORDER BY store_name
