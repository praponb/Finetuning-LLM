SELECT
    level,
    is_remote,
    ROUND(AVG(salary), 2) avg_salary,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(job_title))) job_title_values
FROM employees
GROUP BY CUBE(level, is_remote);
