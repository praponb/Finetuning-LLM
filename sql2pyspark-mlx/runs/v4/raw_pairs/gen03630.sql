SELECT
    CASE WHEN budget >= 3817000 THEN 'high' WHEN budget >= 1410000 THEN 'mid' ELSE 'low' END AS budget_bucket,
    CASE WHEN dept_name IN ('HR', 'Sales') THEN 'group_a' ELSE 'other' END AS dept_name_group,
    CASE WHEN location IN ('London', 'Bangkok') THEN 'priority' ELSE 'other' END AS location_group,
    ROUND(AVG(budget), 2) AS avg_budget
FROM departments
WHERE budget BETWEEN 1121000 AND 3877000
GROUP BY CASE WHEN budget >= 3817000 THEN 'high' WHEN budget >= 1410000 THEN 'mid' ELSE 'low' END, CASE WHEN dept_name IN ('HR', 'Sales') THEN 'group_a' ELSE 'other' END, CASE WHEN location IN ('London', 'Bangkok') THEN 'priority' ELSE 'other' END
ORDER BY location_group DESC;
