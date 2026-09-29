WITH Src AS (
    SELECT *
    FROM departments
    WHERE dept_name IN ('Sales', 'Marketing')
),
summary AS (
    SELECT location, COUNT(*) row_count
    FROM Src
    GROUP BY location
)
SELECT location, row_count
FROM summary
ORDER BY row_count
