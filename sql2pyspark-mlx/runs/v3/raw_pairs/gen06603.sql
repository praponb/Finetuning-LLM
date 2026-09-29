WITH numbered AS (
    SELECT
        id,
        location,
        budget,
        dept_name,
        ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC, id) AS pos
    FROM departments
)
SELECT
    id,
    location,
    budget,
    dept_name
FROM numbered
WHERE pos <= 5
ORDER BY location, budget DESC;
