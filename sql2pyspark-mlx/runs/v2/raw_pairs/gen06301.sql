SELECT id, budget, dept_name FROM departments WHERE budget >= (SELECT MAX(budget) FROM departments WHERE dept_name = 'HR');
