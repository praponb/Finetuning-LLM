SELECT job_title, dept_id
FROM employees
WHERE salary >= 141000
UNION ALL
SELECT job_title, dept_id
FROM employees
WHERE full_name LIKE 'Gra%'
