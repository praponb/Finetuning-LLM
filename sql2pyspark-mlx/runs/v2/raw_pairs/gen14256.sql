WITH ranked AS (
    SELECT
        id,
        dept_name,
        budget,
        location,
        ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget DESC, id) AS row_num
    FROM departments
    WHERE budget > 2230000
)
SELECT *
FROM ranked
WHERE row_num <= 3
ORDER BY id DESC, location DESC
