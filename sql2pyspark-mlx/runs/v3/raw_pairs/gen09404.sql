SELECT supplier_id, brand, MAX(launch_date) AS latest_launch_date, MAX(price) AS max_price FROM products GROUP BY 1, 2 ORDER BY brand
