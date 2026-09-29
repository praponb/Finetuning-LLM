WITH top_departments AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget, id) AS row_num
    FROM departments
)
SELECT id, location, budget
FROM top_departments
WHERE row_num <= 3
ORDER BY budget
