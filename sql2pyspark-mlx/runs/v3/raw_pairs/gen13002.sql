SELECT is_active, MAX(age) - MIN(age) AS age_range FROM customers WHERE age = 63 AND age <= 39 GROUP BY is_active ORDER BY age_range DESC, is_active LIMIT 3;
