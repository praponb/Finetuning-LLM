SELECT is_active, AVG(age) AS avg_age FROM customers GROUP BY is_active ORDER BY avg_age DESC, is_active LIMIT 50;
