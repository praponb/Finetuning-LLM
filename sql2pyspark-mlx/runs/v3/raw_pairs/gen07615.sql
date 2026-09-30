SELECT
    location,
    SUM(budget) AS total_budget,
    COUNT(*) AS cnt,
    DENSE_RANK() OVER (ORDER BY SUM(budget) DESC) AS rnk
FROM departments
WHERE dept_name NOT IN ('Finance', 'Legal', 'Sales')
GROUP BY location
ORDER BY location DESC;
