SELECT
    id,
    dept_name,
    budget,
    location
FROM (
    SELECT
        id,
        dept_name,
        budget,
        location,
        ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget, id) AS rn
    FROM departments
) AS r
WHERE rn <= 2;
