SELECT category, COUNT(DISTINCT price) AS unique_brands, SUM(CASE WHEN brand = 'Acme' THEN 1 ELSE 0 END) AS acme_count, SUM(cost) AS total_cost FROM products GROUP BY category ORDER BY category ASC;
