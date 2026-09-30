SELECT
    d.location,
    COUNT(e.id) AS employee_count,
    SUM(CASE WHEN e.job_title = 'Director' THEN 1 ELSE 0 END) AS director_count,
    COUNT(DISTINCT e.salary) AS unique_levels
FROM departments d
LEFT JOIN employees e ON d.id = e.dept_id
WHERE e.job_title <> 'Manager'
GROUP BY d.location
ORDER BY location DESC
LIMIT 1
