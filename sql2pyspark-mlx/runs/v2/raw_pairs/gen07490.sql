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
        ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget DESC, id) AS row_num
    FROM departments
) AS ranked
WHERE row_num <= 5
ORDER BY budget;
