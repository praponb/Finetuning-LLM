SELECT id, full_name, TRIM(full_name) clean_full_name
FROM employees
WHERE job_title IN ('Analyst', 'Manager') AND email IS NULL AND email LIKE '%@outlook.com'
ORDER BY clean_full_name ASC, id DESC
