SELECT location, tokyo_budget, sales_budget
FROM (
    SELECT location, SUM(CASE WHEN location = 'Tokyo' THEN budget ELSE 0 END) AS tokyo_budget, SUM(CASE WHEN dept_name = 'Sales' THEN budget ELSE 0 END) AS sales_budget
    FROM departments
    GROUP BY location
) AS agg
WHERE tokyo_budget > 41
ORDER BY sales_budget DESC, location
