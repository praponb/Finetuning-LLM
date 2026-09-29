SELECT
    id,
    full_name,
    dept_id,
    manager_id,
    LOWER(email) AS email_lower
FROM employees
ORDER BY 2, id
LIMIT 5;
