WITH numbered AS (
    SELECT
        id,
        location,
        budget,
        dept_name,
        DENSE_RANK() OVER (PARTITION BY location ORDER BY budget DESC) AS row_num
    FROM departments
)
SELECT
    id,
    location,
    budget,
    dept_name
FROM numbered
WHERE row_num <= 5;
