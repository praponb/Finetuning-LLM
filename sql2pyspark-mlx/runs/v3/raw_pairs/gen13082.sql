SELECT MIN(budget) AS min_budget, SUM(budget) AS total_budget
FROM departments
WHERE dept_name IN ('Marketing', 'Legal') AND dept_name IN ('Finance', 'HR', 'Engineering')
ORDER BY min_budget DESC;
