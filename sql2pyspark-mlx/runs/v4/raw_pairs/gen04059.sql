WITH ranked_departments AS (
    SELECT
        id,
        dept_name,
        budget,
        location,
        ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget, id) pos
    FROM departments
    WHERE budget > 1384000
)
SELECT
    id,
    dept_name,
    budget,
    location
FROM ranked_departments
WHERE pos <= 5;
