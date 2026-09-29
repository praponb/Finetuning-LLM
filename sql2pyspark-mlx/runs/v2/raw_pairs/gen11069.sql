SELECT id, budget, dept_name
FROM departments
WHERE budget < (SELECT MIN(budget) FROM departments WHERE budget > 2703000);
