SELECT
    job_title,
    MAX(salary) - MIN(salary) AS salary_range,
    SORT_ARRAY(COLLECT_SET(level)) AS level_list,
    ROUND((100 * SUM(CASE WHEN level = 'L4' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_l4
FROM employees
GROUP BY job_title
