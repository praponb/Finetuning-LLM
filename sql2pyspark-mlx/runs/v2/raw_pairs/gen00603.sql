SELECT level, intern_salary, job_title_list
FROM (
    SELECT level, SUM(CASE WHEN job_title = 'Intern' THEN salary ELSE 0 END) intern_salary, SORT_ARRAY(COLLECT_SET(job_title)) job_title_list
    FROM employees
    WHERE UPPER(full_name) LIKE '%VY %'
    GROUP BY level
) sub
WHERE intern_salary >= 74;
