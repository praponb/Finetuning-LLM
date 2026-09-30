SELECT AVG(budget) avg_budget, SUM(CASE WHEN location = 'New York' THEN 1 ELSE 0 END) new_york_count FROM departments ORDER BY avg_budget DESC;
