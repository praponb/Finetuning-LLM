WITH Numbered AS (
    SELECT
        id,
        dept_name,
        budget,
        ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget, id) AS rnk
    FROM departments
)
SELECT id, dept_name, budget
FROM Numbered
WHERE rnk = 1
