SELECT *
FROM (
    SELECT
        is_remote,
        job_title,
        MAX(salary) AS max_salary,
        SORT_ARRAY(COLLECT_SET(level)) AS level_list
    FROM employees
    GROUP BY is_remote, job_title
) AS t
WHERE max_salary < 148000
ORDER BY max_salary, job_title
