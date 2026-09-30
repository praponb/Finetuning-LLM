WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget, id) AS pos
    FROM departments
)
SELECT *
FROM ranked
WHERE pos = 1;
