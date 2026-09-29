WITH numbered AS (
    SELECT
        id,
        location,
        budget,
        ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC, id) AS row_num
    FROM departments
)
SELECT id, location, budget
FROM numbered
WHERE row_num <= 5;
