SELECT location, total_budget, london_budget
FROM (
    SELECT location, SUM(budget) AS total_budget, SUM(CASE WHEN location = 'London' THEN budget ELSE 0 END) AS london_budget
    FROM departments
    WHERE dept_name IN ('HR', 'Sales')
    GROUP BY location
) AS sub
WHERE london_budget < 54;
