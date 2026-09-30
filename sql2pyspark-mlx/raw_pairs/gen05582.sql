SELECT
    is_remote,
    dept_id,
    l2_count,
    min_hire_date
FROM (
    SELECT
        is_remote,
        dept_id,
        COUNT(CASE WHEN level = 'L2' THEN 1 END) AS l2_count,
        MIN(hire_date) AS min_hire_date
    FROM employees
    WHERE LENGTH(email) < 14
    GROUP BY is_remote, dept_id
) AS agg
WHERE l2_count < 7;
