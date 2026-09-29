SELECT AVG(cost) AS avg_cost, COUNT(CASE WHEN category = 'Home' THEN 1 END) AS home_count FROM products ORDER BY home_count DESC;
