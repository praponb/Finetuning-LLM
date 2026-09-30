SELECT
    d.id,
    d.location,
    d.dept_name,
    t.first_hire_date
FROM departments d
LEFT JOIN (SELECT dept_id, MIN(hire_date) AS first_hire_date
    FROM employees
    GROUP BY dept_id) t ON d.id = t.dept_id;
