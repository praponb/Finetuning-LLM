WITH top_employees AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY dept_id ORDER BY hire_date DESC) AS row_num
    FROM employees
)
SELECT *
FROM top_employees
WHERE row_num <= 3
ORDER BY id DESC;
