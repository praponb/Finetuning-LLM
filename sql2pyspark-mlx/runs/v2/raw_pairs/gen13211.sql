SELECT p.category, ss.units, ss.sale_date FROM products p LEFT JOIN store_sales ss ON ss.product_id = p.id WHERE p.launch_date < '2024-12-01' AND ss.units BETWEEN 34 AND 40 ORDER BY category DESC;
