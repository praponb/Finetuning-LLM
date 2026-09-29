SELECT hire_date, manager_id
FROM employees
WHERE job_title != 'Intern'
UNION
SELECT hire_date, manager_id
FROM employees
WHERE level IN ('L4', 'L5', 'L1')
ORDER BY manager_id
