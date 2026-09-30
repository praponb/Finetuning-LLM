SELECT id, brand FROM products WHERE category NOT IN ('Fashion', 'Sports') UNION ALL SELECT id, brand FROM products WHERE launch_date < '2024-01-01' ORDER BY brand
