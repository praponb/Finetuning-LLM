SELECT dept_name, SUM(budget) AS total_budget FROM departments WHERE dept_name IN ('Sales', 'Legal', 'HR') GROUP BY dept_name HAVING SUM(budget) > 23059000;
