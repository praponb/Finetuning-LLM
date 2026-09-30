SELECT
    e.id,
    e.dept_id AS employee_dept_id,
    m.hire_date AS manager_hire_date,
    m.full_name AS manager_full_name,
    m.is_remote AS manager_is_remote
FROM employees e
INNER JOIN employees m ON e.manager_id = m.id
WHERE m.dept_id = 14
ORDER BY manager_hire_date, id
LIMIT 3;
