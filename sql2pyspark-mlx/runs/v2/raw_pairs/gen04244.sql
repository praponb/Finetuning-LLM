SELECT
    id,
    dept_id,
    hire_date,
    manager_id
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY hire_date DESC, id) AS pos
    FROM employees
    WHERE email IS NOT NULL
) AS r
WHERE pos <= 1
ORDER BY id ASC
