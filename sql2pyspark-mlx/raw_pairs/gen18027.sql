WITH ranked_departments AS (
    SELECT
        id,
        location,
        budget,
        dept_name,
        ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC, id) AS rn
    FROM departments
)
SELECT
    id,
    location,
    budget,
    dept_name
FROM ranked_departments
WHERE rn <= 3
ORDER BY location
