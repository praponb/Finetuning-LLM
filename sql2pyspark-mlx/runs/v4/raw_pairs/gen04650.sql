SELECT CASE WHEN level IN ('L1', 'L4') THEN 'group_a' ELSE 'other' END level_group, ROUND((100 * SUM(CASE WHEN level = 'L2' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_l2
FROM employees
WHERE job_title = 'Analyst' AND manager_id IN (21, 33, 38)
GROUP BY 1
ORDER BY pct_l2 DESC;
