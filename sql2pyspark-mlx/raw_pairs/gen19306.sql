SELECT MIN(budget) min_budget FROM departments WHERE dept_name = 'Legal' AND location = 'Singapore' AND dept_name NOT IN ('Sales', 'Marketing', 'HR') ORDER BY min_budget DESC
