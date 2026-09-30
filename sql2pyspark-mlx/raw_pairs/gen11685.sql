SELECT *
FROM (
    SELECT
        manager_id,
        level,
        salary
    FROM employees
) src
PIVOT (
    SUM(salary) AS amount
    FOR level IN ('L5', 'L2')
);
