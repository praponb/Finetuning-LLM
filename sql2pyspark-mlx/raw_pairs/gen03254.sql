SELECT *
FROM (
    SELECT location, MAX(budget) AS max_budget, AVG(budget) AS avg_budget
    FROM departments
    GROUP BY location
) AS x
WHERE avg_budget > 2948000
ORDER BY avg_budget DESC, max_budget;
