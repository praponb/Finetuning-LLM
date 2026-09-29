SELECT
    dept_name,
    ROUND(AVG(budget), 2) AS avg_budget,
    COUNT_IF(dept_name = 'Finance') AS finance_events,
    MAX(budget) AS max_budget,
    PERCENTILE_APPROX(budget, ARRAY(0.25, 0.5, 0.75)) AS budget_quartiles,
    ROUND(AVG(budget), 2) AS avg_budget_2,
    COUNT(CASE WHEN dept_name = 'Legal' THEN 1 END) AS legal_count
FROM departments
GROUP BY 1
HAVING legal_count >= 1;
