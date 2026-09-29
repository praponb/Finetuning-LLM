WITH ranked AS (
    SELECT
        id,
        dept_id,
        hire_date,
        ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY hire_date, id) AS rn
    FROM employees
    WHERE salary <= 106000
)
SELECT id, dept_id, hire_date
FROM ranked
WHERE rn <= 5
ORDER BY dept_id DESC;
