SELECT
    d.location,
    d.budget,
    d.dept_name,
    e.email
FROM departments d
LEFT OUTER JOIN employees e ON d.id = e.dept_id;
