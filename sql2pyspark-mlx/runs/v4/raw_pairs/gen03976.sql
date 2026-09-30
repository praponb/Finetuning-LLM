SELECT CASE WHEN d.budget >= 4133000 THEN 'high' WHEN d.budget >= 663000 THEN 'mid' ELSE 'low' END AS budget_bucket, CASE WHEN d.dept_name IN ('Marketing', 'Engineering') THEN 'tier_1' ELSE 'other' END AS dept_name_group, ROUND((100 * SUM(CASE WHEN e.level = 'L5' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_l5
FROM employees e
LEFT OUTER JOIN departments d ON e.dept_id = d.id
WHERE UPPER(e.full_name) LIKE '%ACK%'
GROUP BY CASE WHEN d.budget >= 4133000 THEN 'high' WHEN d.budget >= 663000 THEN 'mid' ELSE 'low' END, CASE WHEN d.dept_name IN ('Marketing', 'Engineering') THEN 'tier_1' ELSE 'other' END;
