SELECT id, budget, location, dept_name FROM departments WHERE budget > (SELECT AVG(budget) FROM departments WHERE location NOT IN ('London', 'Chiang Mai'));
