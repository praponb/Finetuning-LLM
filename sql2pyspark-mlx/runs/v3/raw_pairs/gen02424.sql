SELECT
    d.location,
    d.budget,
    d.dept_name,
    e.manager_id,
    e.salary,
    e.dept_id
FROM departments d
LEFT JOIN employees e ON e.dept_id = d.id;
