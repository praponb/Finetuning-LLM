SELECT location, avg_budget
FROM (
    SELECT location, ROUND(AVG(budget), 2) AS avg_budget
    FROM departments
    GROUP BY location
) AS sub
WHERE avg_budget < 27;
