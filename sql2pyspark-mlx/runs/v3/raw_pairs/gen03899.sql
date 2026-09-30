SELECT
    CASE WHEN salary >= 161000 THEN 'high' WHEN salary >= 70000 THEN 'mid' ELSE 'low' END salary_bucket,
    SUM(salary) total_salary,
    COUNT(*) cnt,
    ROUND(SUM(salary) * 100.0 / SUM(SUM(salary)) OVER (), 2) AS pct_of_total
FROM employees
WHERE manager_id IS NOT NULL
GROUP BY CASE WHEN salary >= 161000 THEN 'high' WHEN salary >= 70000 THEN 'mid' ELSE 'low' END
ORDER BY salary_bucket DESC, total_salary
LIMIT 3
