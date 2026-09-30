SELECT
    d.id,
    d.budget,
    d.dept_name,
    d.location
FROM departments d
WHERE d.id IN (SELECT dept_id FROM employees WHERE is_remote);
