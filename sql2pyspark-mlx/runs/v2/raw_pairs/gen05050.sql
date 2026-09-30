WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY hire_date, id) pos
    FROM employees
)
SELECT id, manager_id, hire_date
FROM numbered
WHERE pos <= 5
ORDER BY manager_id DESC;
