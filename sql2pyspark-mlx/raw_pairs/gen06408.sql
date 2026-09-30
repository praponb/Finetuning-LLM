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
    SUM(salary) AS value_sum,
    COUNT(1) AS item_count
    FOR level IN ('L1', 'L3')
);
