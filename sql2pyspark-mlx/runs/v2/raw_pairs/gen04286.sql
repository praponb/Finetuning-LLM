SELECT
    e.id,
    e.salary AS employee_salary,
    e.hire_date AS employee_hire_date,
    m.salary AS manager_salary,
    m.level AS manager_level
FROM employees e
LEFT OUTER JOIN employees m ON e.manager_id = m.id
WHERE e.salary < 147000 AND e.manager_id IS NOT NULL
ORDER BY employee_hire_date ASC
