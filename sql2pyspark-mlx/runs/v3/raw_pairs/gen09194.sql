WITH ranked AS (
    SELECT
        id,
        location,
        budget,
        dept_name,
        ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC, id) AS row_num
    FROM departments
)
SELECT
    id,
    location,
    budget,
    dept_name
FROM ranked
WHERE row_num <= 2
ORDER BY budget DESC
