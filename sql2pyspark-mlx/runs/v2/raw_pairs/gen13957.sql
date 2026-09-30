SELECT id, budget, dept_name
FROM departments
WHERE budget >= (SELECT AVG(budget) FROM departments)
ORDER BY dept_name
