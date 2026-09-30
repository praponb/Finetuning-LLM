WITH ranked AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY dept_name ORDER BY budget DESC) rn
    FROM departments
    WHERE dept_name IN ('Engineering', 'Finance')
)
SELECT *
FROM ranked
WHERE rn <= 3
