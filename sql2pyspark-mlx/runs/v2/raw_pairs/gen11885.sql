SELECT dept_name, ROUND(AVG(budget), 2) AS avg_budget
FROM departments
WHERE (budget >= 1789000 AND dept_name = 'Finance') OR budget > 3165000
GROUP BY dept_name;
