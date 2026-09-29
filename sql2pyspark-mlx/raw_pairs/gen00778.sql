SELECT *
FROM (
    SELECT location, AVG(budget) AS avg_budget
    FROM departments
    GROUP BY location
) AS agg
WHERE avg_budget > 1248000
ORDER BY location;
