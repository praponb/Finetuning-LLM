SELECT
    DATE_TRUNC('MONTH', hire_date) AS activity_month,
    COUNT_IF(level = 'L2') AS l2_events,
    AVG(salary) FILTER (WHERE job_title = 'Intern') AS intern_avg_salary,
    SORT_ARRAY(COLLECT_SET(level)) AS level_list,
    APPROX_PERCENTILE(salary, ARRAY(0.25, 0.5, 0.75)) AS salary_quartiles,
    SORT_ARRAY(COLLECT_SET(job_title)) AS job_title_list
FROM employees
GROUP BY DATE_TRUNC('MONTH', hire_date)
