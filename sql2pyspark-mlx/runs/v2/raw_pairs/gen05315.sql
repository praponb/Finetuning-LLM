SELECT
    job_title,
    MAX(salary) AS max_salary,
    ROUND((100 * SUM(CASE WHEN job_title = 'Director' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_director,
    SUM(CASE WHEN job_title = 'Engineer' THEN 1 ELSE 0 END) AS engineer_count
FROM employees
GROUP BY job_title
HAVING SUM(CASE WHEN job_title = 'Engineer' THEN 1 ELSE 0 END) >= 30;
