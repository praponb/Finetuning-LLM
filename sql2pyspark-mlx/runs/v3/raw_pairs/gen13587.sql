SELECT budget_bucket, dept_name, budget_range
FROM (
    SELECT CASE WHEN budget >= 4052000 THEN 'high' WHEN budget >= 1177000 THEN 'mid' ELSE 'low' END AS budget_bucket, dept_name, MAX(budget) - MIN(budget) AS budget_range
    FROM departments
    GROUP BY CASE WHEN budget >= 4052000 THEN 'high' WHEN budget >= 1177000 THEN 'mid' ELSE 'low' END, dept_name
) AS s
WHERE budget_range >= 56
