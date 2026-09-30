SELECT id, location, budget
FROM (
    SELECT
        id,
        location,
        budget,
        DENSE_RANK() OVER (PARTITION BY location ORDER BY budget DESC) rn
    FROM departments
) ranked
WHERE rn <= 3
ORDER BY budget DESC, location DESC
