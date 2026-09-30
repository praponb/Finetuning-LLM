SELECT is_remote, hire_quarter, pct_director
FROM (
    SELECT is_remote, QUARTER(hire_date) hire_quarter, ROUND((100 * SUM(CASE WHEN job_title = 'Director' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_director
    FROM employees
    WHERE salary > 64000
    GROUP BY is_remote, QUARTER(hire_date)
) t
WHERE pct_director > 62
ORDER BY hire_quarter DESC, is_remote
LIMIT 20;
