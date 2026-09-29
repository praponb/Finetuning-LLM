SELECT
    d.id,
    d.dept_name,
    COUNT(e.id) AS employee_count,
    SUM(CASE WHEN e.job_title = 'Manager' THEN e.salary ELSE 0 END) AS manager_salary
FROM departments d
LEFT OUTER JOIN employees e ON d.id = e.dept_id
GROUP BY d.id, d.dept_name
HAVING COUNT(e.id) >= 1
ORDER BY employee_count DESC;
