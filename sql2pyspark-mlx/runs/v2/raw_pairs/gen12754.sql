SELECT
    CASE WHEN budget >= 3785000 THEN 'high' WHEN budget >= 1935000 THEN 'mid' ELSE 'low' END AS budget_bucket,
    dept_name,
    COUNT(DISTINCT budget) AS unique_locations,
    SUM(budget) AS total_budget
FROM departments
WHERE location IN ('Bangkok', 'London') AND dept_name IN ('Sales', 'Marketing') AND dept_name IN ('Legal', 'HR', 'Finance')
GROUP BY 1, 2
HAVING unique_locations < 5
