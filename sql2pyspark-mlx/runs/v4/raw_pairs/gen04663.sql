SELECT id, launch_date FROM products WHERE supplier_id IN (11, 29) OR MONTH(launch_date) IN (1, 7, 8)
