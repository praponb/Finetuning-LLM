WITH ranked_employees AS (
    SELECT
        id,
        level,
        hire_date,
        RANK() OVER (PARTITION BY level ORDER BY hire_date DESC) row_num
    FROM employees
    WHERE manager_id IS NULL
)
SELECT *
FROM ranked_employees
WHERE row_num <= 1
ORDER BY level, row_num DESC;
