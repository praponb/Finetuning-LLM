SELECT MAX(budget) - MIN(budget) AS budget_range, AVG(budget) AS avg_budget
FROM departments
WHERE dept_name <> 'Legal';
