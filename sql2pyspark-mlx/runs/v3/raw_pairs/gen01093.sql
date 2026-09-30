SELECT id, dept_id, CEIL(salary) AS salary_whole
FROM employees
ORDER BY id DESC
