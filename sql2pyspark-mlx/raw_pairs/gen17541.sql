SELECT
    d.id,
    d.dept_name,
    d.budget,
    d.location
FROM departments d
WHERE d.id IN (SELECT dept_id FROM employees)
