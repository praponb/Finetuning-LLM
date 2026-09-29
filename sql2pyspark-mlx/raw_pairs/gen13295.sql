WITH Ranked AS (
    SELECT
        id,
        location,
        budget,
        ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC, id) AS pos
    FROM departments
)
SELECT id, location, budget
FROM Ranked
WHERE pos <= 3
