SELECT
    e.id,
    e.hire_date AS employee_hire_date,
    e.dept_id AS employee_dept_id,
    m.level AS manager_level
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.id
WHERE e.job_title IN ('Analyst', 'Director', 'Designer') AND m.level <> 'L1'
ORDER BY m.level ASC;
