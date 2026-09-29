SELECT CASE WHEN budget >= 4122000 THEN 'high' WHEN budget >= 2044000 THEN 'mid' ELSE 'low' END budget_bucket, MIN(budget) min_budget, SUM(CASE WHEN dept_name = 'Engineering' THEN 1 ELSE 0 END) engineering_count
FROM departments
WHERE budget BETWEEN 2258000 AND 2686000 AND (location IN ('Bangkok', 'Chiang Mai', 'Tokyo') OR dept_name IN ('Sales', 'Legal'))
GROUP BY 1;
