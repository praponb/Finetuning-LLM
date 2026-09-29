SELECT
    CASE WHEN dept_name IN ('Engineering', 'Marketing') THEN 'tier_1' ELSE 'other' END dept_name_group,
    COUNT(CASE WHEN location = 'Chiang Mai' THEN 1 END) chiang_mai_count,
    ROUND((100 * SUM(CASE WHEN location = 'New York' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_new_york,
    MIN(budget) min_budget
FROM departments
WHERE budget BETWEEN 3281000 AND 3692000 OR dept_name IN ('Engineering', 'Marketing', 'Finance')
GROUP BY 1
HAVING COUNT(*) < 1
ORDER BY chiang_mai_count DESC
