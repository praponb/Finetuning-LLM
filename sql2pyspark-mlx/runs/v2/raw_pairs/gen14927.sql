SELECT id, budget FROM departments WHERE budget > (SELECT AVG(budget) FROM departments WHERE dept_name = 'HR') ORDER BY budget DESC
