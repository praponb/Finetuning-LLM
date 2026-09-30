SELECT id, location, budget
FROM (
    SELECT
        id,
        location,
        budget,
        ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget, id) row_num
    FROM departments
) r
WHERE row_num <= 2
ORDER BY budget DESC
