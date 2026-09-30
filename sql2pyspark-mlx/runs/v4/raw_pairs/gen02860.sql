SELECT
    e.id,
    e.is_remote,
    e.hire_date,
    e.full_name
FROM employees e
WHERE e.dept_id IN (SELECT id FROM departments WHERE dept_name NOT IN ('Marketing', 'Legal', 'Sales'));
