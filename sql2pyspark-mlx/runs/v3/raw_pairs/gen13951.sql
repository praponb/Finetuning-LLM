SELECT
    d.location,
    d.budget,
    e.salary,
    e.dept_id,
    e.manager_id
FROM departments d
LEFT OUTER JOIN employees e ON d.id = e.dept_id;
