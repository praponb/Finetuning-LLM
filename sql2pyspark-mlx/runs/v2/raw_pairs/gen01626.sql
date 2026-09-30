SELECT
    d.budget,
    d.location,
    d.dept_name,
    e.salary
FROM departments d
JOIN employees e ON e.dept_id = d.id;
