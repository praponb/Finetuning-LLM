SELECT id, budget, location FROM departments WHERE budget >= (SELECT AVG(budget) FROM departments WHERE dept_name IS NOT NULL) ORDER BY id;
