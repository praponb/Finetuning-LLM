SELECT location, singapore_budget
FROM (
    SELECT location, SUM(CASE WHEN location = 'Singapore' THEN budget ELSE 0 END) singapore_budget
    FROM departments
    GROUP BY location
) t
WHERE singapore_budget >= 48
ORDER BY location ASC;
