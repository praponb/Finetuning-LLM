SELECT *
FROM (
    SELECT
        manager_id,
        job_title,
        level,
        salary
    FROM employees
) src
PIVOT (
    AVG(salary) AS value_sum
    FOR level IN ('L2', 'L1')
);
