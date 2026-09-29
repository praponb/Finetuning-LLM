WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget, id) AS rnk
    FROM departments
)
SELECT
    id,
    location,
    budget,
    dept_name
FROM numbered
WHERE rnk <= 2
ORDER BY location
