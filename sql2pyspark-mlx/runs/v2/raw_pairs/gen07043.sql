SELECT category, product_name FROM products WHERE launch_date <= '2025-10-01' UNION ALL SELECT category, product_name FROM products WHERE product_name LIKE '%Run%';
