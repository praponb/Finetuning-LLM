SELECT
    e.id,
    e.full_name,
    e.is_remote,
    e.email
FROM employees e
WHERE e.dept_id NOT IN (SELECT id FROM departments WHERE budget < 1711000)
ORDER BY full_name ASC;
