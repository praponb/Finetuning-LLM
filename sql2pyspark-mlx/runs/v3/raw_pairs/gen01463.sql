SELECT is_remote, job_title_values, l4_salary
FROM (
    SELECT is_remote, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(job_title))) AS job_title_values, SUM(CASE WHEN level = 'L4' THEN salary ELSE 0 END) AS l4_salary
    FROM employees
    GROUP BY is_remote
) AS t
WHERE l4_salary >= 26
ORDER BY l4_salary DESC, is_remote
LIMIT 10;
