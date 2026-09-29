WITH ranked_employees AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY job_title ORDER BY hire_date DESC) AS rnk
    FROM employees
    WHERE job_title = 'Engineer'
)
SELECT *
FROM ranked_employees
WHERE rnk <= 3;
