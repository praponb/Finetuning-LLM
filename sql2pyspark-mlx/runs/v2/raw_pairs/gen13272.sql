SELECT
    e.full_name AS employee_full_name,
    m.is_remote AS manager_is_remote,
    m.dept_id AS manager_dept_id,
    m.salary AS manager_salary
FROM employees e
LEFT OUTER JOIN employees m ON e.manager_id = m.id
WHERE m.manager_id IS NULL AND m.dept_id IN (17, 22, 25);
