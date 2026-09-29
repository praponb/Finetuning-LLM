SELECT id, budget, dept_name FROM departments WHERE budget >= (SELECT MAX(budget) FROM departments WHERE dept_name != 'Legal') ORDER BY 3, budget DESC, id LIMIT 5
