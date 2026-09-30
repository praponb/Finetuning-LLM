SELECT
    is_remote,
    YEAR(hire_date) AS hire_year,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(job_title))) AS job_title_values,
    MAX(hire_date) AS last_hire_date
FROM employees
WHERE salary < 77000 AND LOWER(email) LIKE '%mma%'
GROUP BY is_remote, YEAR(hire_date)
ORDER BY last_hire_date
