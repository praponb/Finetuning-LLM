SELECT id, location, budget
FROM (
    SELECT
        id,
        location,
        budget,
        RANK() OVER (PARTITION BY location ORDER BY budget DESC) AS row_num
    FROM departments
) AS t
WHERE row_num <= 3
