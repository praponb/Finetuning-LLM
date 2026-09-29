SELECT
    salary,
    manager_id,
    hire_date,
    SUBSTRING(email, 1, 4) AS email_prefix
FROM employees
WHERE dept_id = 22
ORDER BY email_prefix DESC, manager_id DESC
