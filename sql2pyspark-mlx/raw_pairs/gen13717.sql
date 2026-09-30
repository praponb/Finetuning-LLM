SELECT dept_id, MAX(salary) - MIN(salary) AS salary_range
FROM employees
WHERE is_remote AND dept_id IN (8, 33)
GROUP BY dept_id;
