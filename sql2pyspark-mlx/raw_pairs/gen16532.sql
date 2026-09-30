SELECT CASE WHEN location IN ('Tokyo', 'Berlin') THEN 'core' ELSE 'other' END AS location_group, COUNT(budget) AS budget_count, MAX(budget) AS max_budget
FROM departments
WHERE budget IS NOT NULL
GROUP BY 1
ORDER BY max_budget DESC
