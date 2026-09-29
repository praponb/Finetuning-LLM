WITH top_departments AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget, id) AS rn
    FROM departments
    WHERE dept_name IS NOT NULL
)
SELECT id, dept_name, budget
FROM top_departments
WHERE rn = 1;
