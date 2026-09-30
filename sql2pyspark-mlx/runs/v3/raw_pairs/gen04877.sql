WITH TopDepartments AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget DESC, id) AS rn
    FROM departments
    WHERE dept_name IN ('Marketing', 'Engineering', 'Legal')
)
SELECT id, dept_name, budget
FROM TopDepartments
WHERE rn <= 1;
