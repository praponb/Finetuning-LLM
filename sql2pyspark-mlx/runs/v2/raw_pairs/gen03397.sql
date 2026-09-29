SELECT
    departments.id,
    departments.location,
    departments.budget,
    departments.dept_name
FROM departments
WHERE departments.id IN (SELECT dept_id FROM employees WHERE NOT is_remote)
