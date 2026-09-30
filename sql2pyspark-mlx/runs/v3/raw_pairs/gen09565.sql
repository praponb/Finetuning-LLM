SELECT dept_name, location, COUNT(budget) budget_count
FROM departments
GROUP BY dept_name, location
ORDER BY budget_count ASC, location DESC, dept_name ASC
LIMIT 1;
