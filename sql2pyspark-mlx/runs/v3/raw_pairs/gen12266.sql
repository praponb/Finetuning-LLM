SELECT
    d.location,
    COUNT(e.id) AS employee_count,
    ROUND(AVG(e.salary), 2) AS avg_salary,
    MIN(e.hire_date) AS min_hire_date
FROM departments d
JOIN employees e ON d.id = e.dept_id
GROUP BY d.location
