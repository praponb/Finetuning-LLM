SELECT id, job_title, salary
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY salary DESC, id) AS pos
    FROM employees
) AS r
WHERE pos <= 3;
