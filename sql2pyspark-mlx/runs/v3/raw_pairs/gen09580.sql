SELECT
    id,
    manager_id,
    salary,
    is_remote
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY salary, id) AS pos
    FROM employees
    WHERE dept_id IN (34, 38, 39)
) AS t
WHERE pos <= 3
ORDER BY manager_id DESC
