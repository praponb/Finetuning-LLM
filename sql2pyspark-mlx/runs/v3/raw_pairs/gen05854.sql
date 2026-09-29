WITH ranked_departments AS (
    SELECT
        id,
        location,
        budget,
        dept_name,
        RANK() OVER (PARTITION BY location ORDER BY budget) AS row_num
    FROM departments
)
SELECT *
FROM ranked_departments
WHERE row_num <= 5;
