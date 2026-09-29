WITH ranked AS (
    SELECT
        id,
        manager_id,
        hire_date,
        RANK() OVER (PARTITION BY manager_id ORDER BY hire_date DESC) AS pos
    FROM employees
    WHERE dept_id = 16
)
SELECT id, manager_id, hire_date
FROM ranked
WHERE pos = 1
ORDER BY hire_date, id DESC;
