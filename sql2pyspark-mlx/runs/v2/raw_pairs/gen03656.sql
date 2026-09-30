SELECT dept_name, ROUND(STDDEV(budget), 2) AS stddev_budget, SUM(CASE WHEN location = 'Chiang Mai' THEN 1 ELSE 0 END) AS chiang_mai_count
FROM departments
GROUP BY dept_name
ORDER BY chiang_mai_count;
