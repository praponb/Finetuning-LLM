WITH Base AS (
    SELECT *
    FROM employees
    WHERE salary >= 138000
),
agg AS (
    SELECT level, SORT_ARRAY(COLLECT_SET(job_title)) AS job_title_list
    FROM Base
    GROUP BY level
)
SELECT *
FROM agg
ORDER BY level DESC
