SELECT id, dept_name, budget
FROM (
    SELECT
        id,
        dept_name,
        budget,
        ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget DESC, id) pos
    FROM departments
    WHERE dept_name NOT IN ('Engineering', 'Sales')
) t
WHERE pos <= 2;
