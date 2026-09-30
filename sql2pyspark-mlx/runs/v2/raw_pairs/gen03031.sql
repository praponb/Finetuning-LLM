SELECT
    CASE WHEN d.budget >= 3839000 THEN 'high' WHEN d.budget >= 1551000 THEN 'mid' ELSE 'low' END AS budget_bucket,
    d.dept_name,
    MAX(e.hire_date) AS last_hire_date,
    COUNT(CASE WHEN e.level = 'L1' THEN 1 END) AS l1_count
FROM employees e
LEFT OUTER JOIN departments d ON e.dept_id = d.id
WHERE d.location IN ('London', 'Singapore', 'Bangkok')
GROUP BY CASE WHEN d.budget >= 3839000 THEN 'high' WHEN d.budget >= 1551000 THEN 'mid' ELSE 'low' END, d.dept_name
ORDER BY dept_name;
