WITH top_departments AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC, id) AS row_num
    FROM departments
)
SELECT
    id,
    location,
    budget,
    dept_name
FROM top_departments
WHERE row_num = 1
ORDER BY budget DESC
