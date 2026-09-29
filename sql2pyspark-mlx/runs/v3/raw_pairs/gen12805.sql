SELECT id, dept_name, budget
FROM (
    SELECT
        id,
        dept_name,
        budget,
        ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget DESC, id) AS pos
    FROM departments
) AS ranked
WHERE pos <= 5
ORDER BY id
