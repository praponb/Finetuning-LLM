SELECT id, level, dept_id
FROM employees
WHERE job_title IN ('Intern', 'Manager')
UNION
SELECT id, level, dept_id
FROM employees
WHERE UPPER(TRIM(full_name)) = 'BOB SMITH'
ORDER BY level;
