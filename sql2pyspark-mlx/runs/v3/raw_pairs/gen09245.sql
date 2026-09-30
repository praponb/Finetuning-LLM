SELECT
    CASE WHEN budget >= 3427000 THEN 'high' WHEN budget >= 1159000 THEN 'mid' ELSE 'low' END AS budget_bucket,
    CASE WHEN location IN ('Singapore', 'London') THEN 'priority' ELSE 'other' END AS location_group,
    SUM(CASE WHEN dept_name = 'Sales' THEN budget ELSE 0 END) AS sales_budget,
    MAX(budget) AS max_budget
FROM departments
GROUP BY budget_bucket, location_group
HAVING SUM(CASE WHEN dept_name = 'Sales' THEN budget ELSE 0 END) > 49
