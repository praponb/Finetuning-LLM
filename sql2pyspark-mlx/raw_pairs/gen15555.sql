WITH top_departments AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY location ORDER BY budget) AS rn
    FROM departments
)
SELECT
    id,
    location,
    budget,
    dept_name
FROM top_departments
WHERE rn <= 3
