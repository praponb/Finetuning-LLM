WITH top_employees AS (
    SELECT
        id,
        level,
        salary,
        dept_id,
        ROW_NUMBER() OVER (PARTITION BY level ORDER BY salary DESC, id) AS rn
    FROM employees
)
SELECT
    id,
    level,
    salary,
    dept_id
FROM top_employees
WHERE rn = 1
ORDER BY salary
