SELECT CASE WHEN d.budget >= 3404000 THEN 'high' WHEN d.budget >= 1558000 THEN 'mid' ELSE 'low' END AS budget_bucket, COUNT(CASE WHEN e.level = 'L5' THEN 1 END) AS l5_count
FROM employees e
JOIN departments d ON e.dept_id = d.id
GROUP BY CASE WHEN d.budget >= 3404000 THEN 'high' WHEN d.budget >= 1558000 THEN 'mid' ELSE 'low' END
ORDER BY l5_count DESC;
