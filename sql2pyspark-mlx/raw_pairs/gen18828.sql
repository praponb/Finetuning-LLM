SELECT
    d.location,
    a.level,
    COUNT(*) AS cnt,
    AVG(a.salary) AS avg_salary,
    GROUPING_ID() AS level_id
FROM employees a
LEFT JOIN departments d ON a.dept_id = d.id
GROUP BY CUBE (d.location, a.level);
