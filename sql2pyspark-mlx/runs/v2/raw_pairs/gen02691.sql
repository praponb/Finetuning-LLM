SELECT id, budget, dept_name, location FROM departments WHERE budget <= (SELECT MIN(budget) FROM departments)
