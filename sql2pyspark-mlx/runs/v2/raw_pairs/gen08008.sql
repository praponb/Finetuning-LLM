SELECT category, brand FROM products WHERE launch_date >= '2024-01-01' UNION SELECT category, brand FROM products WHERE category NOT IN ('Grocery', 'Electronics');
