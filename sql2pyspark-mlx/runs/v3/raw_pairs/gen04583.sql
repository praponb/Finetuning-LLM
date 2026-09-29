WITH top_departments AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY dept_name ORDER BY budget DESC) AS rn
    FROM departments
)
SELECT *
FROM top_departments
WHERE rn = 1;
