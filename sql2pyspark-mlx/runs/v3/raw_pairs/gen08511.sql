SELECT id, category FROM products WHERE MONTH(launch_date) IN (1, 9, 10) INTERSECT SELECT id, category FROM products WHERE category = 'Electronics'
