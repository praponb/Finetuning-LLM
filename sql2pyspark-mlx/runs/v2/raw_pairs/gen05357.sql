WITH ranked_employees AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY level ORDER BY hire_date DESC) AS pos
    FROM employees
    WHERE hire_date >= '2024-11-07'
)
SELECT
    id,
    level,
    hire_date,
    full_name
FROM ranked_employees
WHERE pos <= 3
ORDER BY full_name;
