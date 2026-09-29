SELECT dept_name, SUM(budget) AS total_budget FROM departments WHERE dept_name IS NOT NULL AND dept_name IN ('Engineering', 'HR') GROUP BY dept_name ORDER BY dept_name DESC LIMIT 20;
