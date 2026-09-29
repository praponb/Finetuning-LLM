SELECT
    e.dept_id,
    d.dept_name,
    d.location,
    d.budget
FROM employees e
INNER JOIN departments d ON e.dept_id = d.id
WHERE d.dept_name IN ('Marketing', 'Legal', 'HR') AND e.hire_date <= '2025-10-01';
