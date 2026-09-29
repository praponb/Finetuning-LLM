SELECT
    d.id,
    d.dept_name,
    d.location,
    d.budget
FROM departments d
WHERE d.dept_name = 'Legal' AND d.id IN (SELECT dept_id FROM employees WHERE hire_date <= '2025-10-15')
