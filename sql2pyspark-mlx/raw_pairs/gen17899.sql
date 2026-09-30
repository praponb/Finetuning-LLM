SELECT
    CASE WHEN salary >= 132000 THEN 'LARGE' ELSE 'SMALL' END AS salary_tier,
    SORT_ARRAY(COLLECT_SET(job_title)) AS job_title_list,
    MAX(salary) AS max_salary,
    SORT_ARRAY(COLLECT_SET(level)) AS level_list,
    COUNT(DISTINCT id) AS distinct_ids
FROM employees
GROUP BY CASE WHEN salary >= 132000 THEN 'LARGE' ELSE 'SMALL' END;
