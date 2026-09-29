SELECT
    dept_name,
    location,
    SUM(CASE WHEN dept_name = 'Legal' THEN 1 ELSE 0 END) AS legal_count,
    ROUND(AVG(budget), 2) AS avg_budget
FROM departments
WHERE dept_name IN ('Legal', 'Sales', 'Marketing')
GROUP BY dept_name, location;
