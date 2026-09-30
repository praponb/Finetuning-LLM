SELECT launch_date FROM products WHERE brand <> 'Acme' UNION SELECT sale_date FROM store_sales ORDER BY launch_date;
