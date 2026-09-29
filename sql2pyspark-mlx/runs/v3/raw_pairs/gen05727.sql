SELECT
    d.id,
    d.budget,
    d.dept_name,
    d.location
FROM departments d
WHERE NOT EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id AND e.hire_date BETWEEN '2023-04-09' AND '2023-08-22')
