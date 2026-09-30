WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget DESC, id) AS rnk
    FROM departments
    WHERE budget > 3717000
)
SELECT
    id,
    dept_name,
    budget,
    location
FROM ranked
WHERE rnk <= 1
ORDER BY id;
