SELECT
    dept_name,
    COUNT(budget) AS budget_count,
    MAX(budget) - MIN(budget) AS budget_range,
    SUM(CASE WHEN location = 'Singapore' THEN budget ELSE 0 END) AS singapore_budget
FROM departments
GROUP BY 1
ORDER BY budget_count DESC, budget_range DESC;
