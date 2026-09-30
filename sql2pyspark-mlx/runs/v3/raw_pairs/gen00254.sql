SELECT id, location, budget
FROM (
    SELECT
        id,
        location,
        budget,
        DENSE_RANK() OVER (PARTITION BY location ORDER BY budget DESC) AS rn
    FROM departments
) AS r
WHERE rn <= 3
ORDER BY id;
