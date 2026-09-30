SELECT *
FROM (
    SELECT location, ROUND(STDDEV(budget), 2) stddev_budget
    FROM departments
    GROUP BY location
) sub
WHERE stddev_budget >= 49
ORDER BY location, stddev_budget DESC
LIMIT 5;
