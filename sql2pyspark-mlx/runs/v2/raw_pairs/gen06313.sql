WITH recent_employees AS (
    SELECT *
    FROM employees
    WHERE job_title = 'Designer'
),
agg AS (
    SELECT manager_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(level))) AS level_values, ROUND(STDDEV(salary), 2) AS stddev_salary
    FROM recent_employees
    GROUP BY manager_id
)
SELECT *
FROM agg;
