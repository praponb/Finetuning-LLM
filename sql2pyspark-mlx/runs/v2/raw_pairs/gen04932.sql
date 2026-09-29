WITH top_employees AS (
    SELECT
        id,
        dept_id,
        hire_date,
        manager_id,
        RANK() OVER (PARTITION BY dept_id ORDER BY hire_date) AS rn
    FROM employees
)
SELECT
    id,
    dept_id,
    hire_date,
    manager_id
FROM top_employees
WHERE rn <= 2;
