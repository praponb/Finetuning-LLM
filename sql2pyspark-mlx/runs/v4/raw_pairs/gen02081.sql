SELECT category, MIN(cost) AS min_cost, COUNT(*) AS product_count FROM products WHERE product_name LIKE '%Smar%' GROUP BY category ORDER BY min_cost;
