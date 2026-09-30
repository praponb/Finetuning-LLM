SELECT job_title, manager_id
FROM employees
WHERE UPPER(email) LIKE '%ARO%'
UNION ALL
SELECT job_title, manager_id
FROM employees
WHERE LENGTH(email) > 6;
