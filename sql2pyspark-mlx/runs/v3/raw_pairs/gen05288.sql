SELECT id, level, hire_date
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY hire_date, id) rnk
    FROM employees
    WHERE job_title IN ('Designer', 'Engineer', 'Director')
) ranked
WHERE rnk <= 5
ORDER BY level DESC
