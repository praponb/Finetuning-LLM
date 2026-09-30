SELECT location, dept_name, MIN(budget) min_budget, COUNT(CASE WHEN location = 'Tokyo' THEN 1 END) tokyo_count FROM departments GROUP BY location, dept_name HAVING MIN(budget) > 1328000
