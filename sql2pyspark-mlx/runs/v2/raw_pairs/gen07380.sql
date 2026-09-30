SELECT
    is_remote,
    job_title,
    SUM(salary) AS total_salary,
    ROUND(STDDEV(salary), 2) AS stddev_salary
FROM employees
WHERE email LIKE '%@outlook.com' AND job_title IN ('Designer', 'Analyst')
GROUP BY is_remote, job_title
ORDER BY job_title DESC
