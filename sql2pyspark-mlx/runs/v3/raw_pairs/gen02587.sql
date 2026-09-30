WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget DESC, id) AS row_num
    FROM departments
)
SELECT id, dept_name, budget
FROM ranked
WHERE row_num = 1
ORDER BY dept_name ASC, budget DESC;
