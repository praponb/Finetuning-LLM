SELECT
    CASE WHEN budget >= 3809000 THEN 'high' WHEN budget >= 1786000 THEN 'mid' ELSE 'low' END budget_bucket,
    location,
    COUNT(CASE WHEN location = 'Bangkok' THEN 1 END) bangkok_count,
    COUNT(*) cnt,
    ROUND(STDDEV(budget), 2) stddev_budget
FROM departments
WHERE budget < 3926000 AND dept_name IN ('Legal', 'Finance')
GROUP BY CASE WHEN budget >= 3809000 THEN 'high' WHEN budget >= 1786000 THEN 'mid' ELSE 'low' END, location
ORDER BY 3 DESC;
