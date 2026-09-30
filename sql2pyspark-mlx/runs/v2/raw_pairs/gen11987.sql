SELECT id, level, hire_date
FROM employees
WHERE manager_id IN (24, 28)
UNION
SELECT id, level, hire_date
FROM employees
WHERE job_title IN ('Intern', 'Engineer', 'Manager')
ORDER BY level;
