SELECT budget, CASE WHEN budget >= 3766000 THEN 'High' WHEN budget >= 1592000 THEN 'Medium' ELSE 'Low' END budget_tier
FROM departments
WHERE budget BETWEEN 1567000 AND 3759000
ORDER BY budget DESC
