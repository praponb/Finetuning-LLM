SELECT
    CASE WHEN salary >= 144000 THEN 'high' WHEN salary >= 56000 THEN 'mid' ELSE 'low' END AS salary_bucket,
    SUM(salary) AS total_salary,
    COUNT(*) AS cnt,
    ROUND(SUM(salary) * 100.0 / SUM(SUM(salary)) OVER (), 2) AS pct_of_total
FROM employees
GROUP BY CASE WHEN salary >= 144000 THEN 'high' WHEN salary >= 56000 THEN 'mid' ELSE 'low' END
ORDER BY total_salary DESC, pct_of_total DESC, salary_bucket ASC
LIMIT 20;
