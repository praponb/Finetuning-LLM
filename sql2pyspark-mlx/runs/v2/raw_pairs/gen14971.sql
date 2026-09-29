SELECT
    e.id,
    e.is_remote,
    e.full_name,
    e.hire_date
FROM employees e
WHERE e.dept_id NOT IN (SELECT id FROM departments WHERE dept_name <> 'Legal');
