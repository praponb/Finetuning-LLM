WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC, id) AS rnk
    FROM departments
)
SELECT id, location, budget
FROM ranked
WHERE rnk <= 2
ORDER BY id DESC, budget DESC
