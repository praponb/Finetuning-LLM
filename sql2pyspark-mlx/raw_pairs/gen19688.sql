SELECT category FROM products WHERE category IS NOT NULL UNION ALL SELECT city FROM stores WHERE city IS NOT NULL;
