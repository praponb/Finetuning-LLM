SELECT ROUND(STDDEV(budget), 2) AS stddev_budget, SUM(CASE WHEN dept_name = 'Engineering' THEN 1 ELSE 0 END) AS engineering_count
FROM departments
ORDER BY engineering_count DESC
