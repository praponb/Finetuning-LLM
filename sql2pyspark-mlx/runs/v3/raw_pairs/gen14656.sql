SELECT *
FROM (
    SELECT
        id,
        dept_id,
        hire_date,
        job_title,
        ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY hire_date DESC, id) AS pos
    FROM employees
    WHERE is_remote
) AS r
WHERE pos <= 3;
