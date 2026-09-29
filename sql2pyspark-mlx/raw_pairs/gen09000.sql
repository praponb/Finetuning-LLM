WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC, id) AS pos
    FROM departments
)
SELECT id, location, budget
FROM ranked
WHERE pos <= 1
