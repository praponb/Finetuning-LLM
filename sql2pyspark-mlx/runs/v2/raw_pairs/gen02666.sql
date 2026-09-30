SELECT dept_id, MAX(salary) - MIN(salary) salary_range
FROM employees
WHERE salary <= 109000
GROUP BY dept_id;
