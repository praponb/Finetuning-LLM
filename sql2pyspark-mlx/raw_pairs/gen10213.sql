SELECT id, dept_id, REGEXP_REPLACE(email, ' ', '-') AS email_slug
FROM employees
WHERE manager_id IS NULL
