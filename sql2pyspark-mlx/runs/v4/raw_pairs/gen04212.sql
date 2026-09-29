SELECT CASE WHEN budget >= 3555000 THEN 'high' WHEN budget >= 1399000 THEN 'mid' ELSE 'low' END AS budget_bucket, COUNT(DISTINCT budget) AS unique_dept_names FROM departments GROUP BY budget_bucket
