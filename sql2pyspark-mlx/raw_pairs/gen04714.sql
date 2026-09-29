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
        DENSE_RANK() OVER (PARTITION BY dept_name ORDER BY budget) AS row_num
    FROM departments
) AS r
WHERE row_num <= 3
ORDER BY 4;
