SELECT
    e.id,
    e.is_remote AS employee_is_remote,
    e.manager_id AS employee_manager_id,
    e.dept_id AS employee_dept_id,
    m.dept_id AS manager_dept_id,
    m.salary AS manager_salary,
    INITCAP(TRIM(m.level)) AS level_title
FROM employees e
INNER JOIN employees m ON e.manager_id = m.id
WHERE e.salary BETWEEN 114000 AND 130000 AND LOWER(e.full_name) LIKE '%lic%'
ORDER BY employee_dept_id;
