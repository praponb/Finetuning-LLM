WITH top_employees AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY hire_date DESC, id) pos
    FROM employees
)
SELECT id, manager_id, hire_date
FROM top_employees
WHERE pos <= 2
