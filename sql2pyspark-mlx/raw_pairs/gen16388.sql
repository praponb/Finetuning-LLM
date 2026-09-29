WITH RankedDepartments AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget, id) AS pos
    FROM departments
    WHERE budget < 1152000
)
SELECT
    id,
    dept_name,
    budget,
    location
FROM RankedDepartments
WHERE pos <= 1
