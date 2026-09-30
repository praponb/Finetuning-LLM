SELECT *
FROM (
    SELECT CASE WHEN budget >= 4213000 THEN 'high' WHEN budget >= 1355000 THEN 'mid' ELSE 'low' END AS budget_bucket, SUM(CASE WHEN dept_name = 'Finance' THEN budget ELSE 0 END) AS finance_budget, COUNT(CASE WHEN dept_name = 'Legal' THEN 1 END) AS legal_count
    FROM departments
    GROUP BY CASE WHEN budget >= 4213000 THEN 'high' WHEN budget >= 1355000 THEN 'mid' ELSE 'low' END
) AS agg
WHERE legal_count < 4
