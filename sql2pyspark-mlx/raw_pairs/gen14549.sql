WITH RankedDepartments AS (
    SELECT
        id,
        dept_name,
        budget,
        location,
        ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget DESC, id) AS rn
    FROM departments
)
SELECT
    id,
    dept_name,
    budget,
    location
FROM RankedDepartments
WHERE rn <= 5;
