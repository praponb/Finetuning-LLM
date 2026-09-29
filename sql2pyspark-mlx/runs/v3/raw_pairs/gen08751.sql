SELECT *
FROM (
    SELECT
        id,
        manager_id,
        salary,
        is_remote,
        ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY salary, id) AS pos
    FROM employees
) AS t
WHERE pos <= 3
ORDER BY salary DESC, pos ASC
