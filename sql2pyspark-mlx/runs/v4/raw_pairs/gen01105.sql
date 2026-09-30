SELECT *
FROM (
    SELECT
        level,
        manager_id,
        job_title,
        salary
    FROM employees
) src
PIVOT (
    AVG(salary) AS amount,
    COUNT(1) AS cnt
    FOR job_title IN ('Analyst', 'Engineer', 'Director')
);
