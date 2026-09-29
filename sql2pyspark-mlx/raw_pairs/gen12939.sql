WITH numbered AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY dept_id ORDER BY hire_date DESC) AS rnk
    FROM employees
    WHERE full_name IS NOT NULL
)
SELECT
    id,
    dept_id,
    hire_date,
    email
FROM numbered
WHERE rnk <= 3;
