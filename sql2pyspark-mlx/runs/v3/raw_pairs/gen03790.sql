WITH Numbered AS (
    SELECT
        id,
        dept_name,
        budget,
        location,
        ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget DESC, id) AS rn
    FROM departments
)
SELECT *
FROM Numbered
WHERE rn <= 5;
