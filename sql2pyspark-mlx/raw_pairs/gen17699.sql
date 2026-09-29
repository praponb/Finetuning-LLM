WITH top_departments AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget, id) AS rnk
    FROM departments
)
SELECT id, location, budget
FROM top_departments
WHERE rnk <= 3
