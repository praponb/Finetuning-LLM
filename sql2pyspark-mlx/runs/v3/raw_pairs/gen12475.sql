SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY salary, id) AS pos
    FROM employees
) AS t
WHERE pos <= 3
ORDER BY salary;
