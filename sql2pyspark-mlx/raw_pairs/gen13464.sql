WITH ranked_employees AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY salary, id) AS rn
    FROM employees
)
SELECT
    id,
    level,
    salary,
    hire_date
FROM ranked_employees
WHERE rn <= 2;
