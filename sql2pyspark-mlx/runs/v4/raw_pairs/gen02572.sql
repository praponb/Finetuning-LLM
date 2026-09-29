SELECT
    is_remote,
    job_title,
    COUNT(CASE WHEN job_title = 'Director' THEN 1 END) AS director_count,
    MAX(salary) - MIN(salary) AS salary_range
FROM employees
WHERE level = 'L3'
GROUP BY is_remote, job_title
HAVING MAX(salary) - MIN(salary) > 75
ORDER BY job_title DESC, is_remote
LIMIT 50
