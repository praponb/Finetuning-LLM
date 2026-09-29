SELECT *
FROM (
    SELECT
        job_title,
        dept_id,
        SUM(CASE WHEN level = 'L4' THEN 1 ELSE 0 END) AS l4_count,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(level))) AS level_values
    FROM employees
    GROUP BY job_title, dept_id
) AS x
WHERE l4_count < 55;
