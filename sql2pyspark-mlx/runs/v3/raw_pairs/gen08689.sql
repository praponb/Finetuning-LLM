SELECT
    level,
    COUNT(CASE WHEN level = 'L2' THEN 1 END) AS l2_count,
    ROUND((100 * SUM(CASE WHEN job_title = 'Designer' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_designer,
    MIN(salary) AS min_salary
FROM employees
WHERE dept_id IN (1, 25, 31)
GROUP BY level
HAVING ROUND((100 * SUM(CASE WHEN job_title = 'Designer' THEN 1 ELSE 0 END)) / COUNT(*), 1) < 68
ORDER BY 3 DESC;
