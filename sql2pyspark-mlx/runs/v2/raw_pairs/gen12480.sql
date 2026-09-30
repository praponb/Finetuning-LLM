SELECT DISTINCT job_title
FROM employees
WHERE manager_id IS NOT NULL;
