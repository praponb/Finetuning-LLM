SELECT
    d.dept_name,
    e.hire_date,
    e.salary,
    e.level
FROM departments d
INNER JOIN employees e ON e.dept_id = d.id
WHERE NOT e.is_remote
ORDER BY e.level DESC
