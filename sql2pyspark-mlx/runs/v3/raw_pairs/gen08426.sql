SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY salary, id) AS rnk
    FROM employees
) AS ranked
WHERE rnk <= 2
ORDER BY email;
