SELECT dept_name, CASE WHEN budget >= 3526000 THEN 'high' WHEN budget >= 1185000 THEN 'mid' ELSE 'low' END AS budget_bucket, MIN(budget) AS min_budget
FROM departments
WHERE dept_name IN ('Engineering', 'Marketing') AND location IN ('Chiang Mai', 'Bangkok')
GROUP BY 1, 2
ORDER BY min_budget DESC;
