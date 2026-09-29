SELECT
    d.id,
    d.dept_name,
    COUNT(e.id) AS employee_count,
    COUNT(CASE WHEN e.job_title = 'Manager' THEN 1 END) AS manager_count,
    SUM(CASE WHEN e.job_title = 'Designer' THEN e.salary ELSE 0 END) AS designer_salary
FROM departments d
LEFT JOIN employees e ON d.id = e.dept_id
GROUP BY d.id, d.dept_name;
