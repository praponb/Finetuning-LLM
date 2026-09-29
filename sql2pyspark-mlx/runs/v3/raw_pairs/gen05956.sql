WITH Ranked AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY dept_name ORDER BY budget DESC) AS rnk
    FROM departments
)
SELECT
    id,
    dept_name,
    budget,
    location
FROM Ranked
WHERE rnk = 1
