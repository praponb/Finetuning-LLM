SELECT CASE WHEN budget >= 3374000 THEN 'high' WHEN budget >= 1657000 THEN 'mid' ELSE 'low' END AS budget_bucket, MIN(budget) AS min_budget
FROM departments
WHERE location IN ('Berlin', 'Tokyo') AND dept_name IN ('Finance', 'Sales') AND budget >= 3434000
GROUP BY CASE WHEN budget >= 3374000 THEN 'high' WHEN budget >= 1657000 THEN 'mid' ELSE 'low' END;
