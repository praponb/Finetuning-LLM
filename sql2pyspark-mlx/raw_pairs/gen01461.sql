WITH recent_employees AS (
    SELECT *
    FROM employees
    WHERE NOT (job_title <> 'Analyst' AND dept_id IN (17, 28, 35, 39))
),
agg AS (
    SELECT level, ROUND(STDDEV(salary), 2) AS stddev_salary, SORT_ARRAY(COLLECT_SET(job_title)) AS job_title_list
    FROM recent_employees
    GROUP BY level
)
SELECT *
FROM agg
WHERE stddev_salary > 42
