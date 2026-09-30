SELECT
    d.id,
    d.location,
    COUNT(e.id) AS employee_count,
    ROUND(AVG(e.salary), 2) AS avg_salary,
    SUM(CASE WHEN e.level = 'L3' THEN e.salary ELSE 0 END) AS l3_salary
FROM departments d
LEFT JOIN employees e ON d.id = e.dept_id
WHERE e.full_name LIKE 'I%'
GROUP BY d.id, d.location
ORDER BY employee_count DESC;
