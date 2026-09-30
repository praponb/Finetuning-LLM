SELECT
    level,
    is_remote,
    job_title,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(job_title))) AS job_title_values,
    ROUND(STDDEV(salary), 2) AS stddev_salary
FROM employees
WHERE hire_date > '2025-02-01' OR job_title NOT IN ('Analyst', 'Director')
GROUP BY level, is_remote, job_title
ORDER BY job_title, level, is_remote
LIMIT 10;
