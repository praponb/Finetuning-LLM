SELECT
    departments.id,
    departments.dept_name,
    departments.budget,
    departments.location
FROM departments
WHERE departments.id IN (SELECT dept_id FROM employees WHERE dept_id IN (9, 18, 36))
ORDER BY location;
