SELECT
    e.id,
    e.manager_id AS employee_manager_id,
    m.salary AS manager_salary,
    m.manager_id AS manager_manager_id,
    m.dept_id AS manager_dept_id
FROM employees e
INNER JOIN employees m ON e.manager_id = m.id
WHERE e.level IS NULL
ORDER BY manager_salary
