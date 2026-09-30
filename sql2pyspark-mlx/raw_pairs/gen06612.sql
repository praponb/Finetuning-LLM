SELECT
    d.location,
    d.dept_name,
    COUNT(*) AS num_rows,
    SUM(src.salary) AS total_salary,
    GROUPING_ID() AS grouping_level
FROM employees src
JOIN departments d ON src.dept_id = d.id
GROUP BY GROUPING SETS ((d.location, d.dept_name), (d.location))
ORDER BY dept_name NULLS FIRST;
