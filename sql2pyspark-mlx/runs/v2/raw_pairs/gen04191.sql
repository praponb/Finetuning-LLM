SELECT CASE WHEN budget >= 3379000 THEN 'high' WHEN budget >= 1717000 THEN 'mid' ELSE 'low' END AS budget_bucket, ROUND(STDDEV(budget), 2) AS stddev_budget
FROM departments
WHERE dept_name = 'Legal' AND budget >= 3824000
GROUP BY CASE WHEN budget >= 3379000 THEN 'high' WHEN budget >= 1717000 THEN 'mid' ELSE 'low' END;
