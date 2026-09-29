WITH ranked AS (
    SELECT
        id,
        dept_name,
        budget,
        RANK() OVER (PARTITION BY dept_name ORDER BY budget DESC) AS rnk
    FROM departments
)
SELECT id, dept_name, budget
FROM ranked
WHERE rnk <= 3
ORDER BY dept_name;
