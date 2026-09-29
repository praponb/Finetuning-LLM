SELECT dept_name, COUNT(budget) budget_count, COUNT(*) cnt
FROM departments
GROUP BY dept_name
HAVING COUNT(budget) < 1;
