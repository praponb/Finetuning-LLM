SELECT country, MAX(age) - MIN(age) AS age_range FROM customers WHERE age IS NOT NULL OR city IN ('London', 'Chiang Mai', 'Tokyo') GROUP BY 1 ORDER BY country LIMIT 5;
