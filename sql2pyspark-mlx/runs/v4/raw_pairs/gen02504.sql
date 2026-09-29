SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title, level ORDER BY salary, id) AS latest_rank
    FROM employees
) x
WHERE latest_rank = 1;
