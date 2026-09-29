SELECT
    e.id,
    e.email,
    e.hire_date,
    d.dept_name,
    d.budget,
    SUBSTRING(e.level, 1, 4) AS level_prefix
FROM employees e
RIGHT OUTER JOIN departments d ON e.dept_id = d.id
ORDER BY 1 ASC, 4 ASC
