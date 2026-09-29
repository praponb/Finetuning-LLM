SELECT
    id,
    manager_id,
    salary,
    dept_id
FROM (
    SELECT
        id,
        manager_id,
        salary,
        dept_id,
        ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY salary, id) pos
    FROM employees
) t
WHERE pos = 1
