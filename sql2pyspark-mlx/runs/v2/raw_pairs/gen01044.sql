SELECT id, manager_id, FLOOR(salary) AS salary_whole
FROM employees
WHERE job_title = 'Designer'
ORDER BY manager_id DESC
