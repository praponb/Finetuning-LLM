SELECT
    e.dept_id,
    d.dept_name,
    d.location,
    d.budget,
    CONCAT(e.full_name, ' - ', e.email) AS full_name_email
FROM employees e
JOIN departments d ON e.dept_id = d.id;
