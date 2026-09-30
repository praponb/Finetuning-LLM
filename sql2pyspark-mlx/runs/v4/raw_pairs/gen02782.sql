SELECT *
FROM (
    SELECT CASE WHEN salary >= 161000 THEN 'high' WHEN salary >= 68000 THEN 'mid' ELSE 'low' END AS salary_bucket, ROUND(AVG(salary), 2) AS avg_salary
    FROM employees
    GROUP BY CASE WHEN salary >= 161000 THEN 'high' WHEN salary >= 68000 THEN 'mid' ELSE 'low' END
) AS x
WHERE avg_salary >= 62
ORDER BY salary_bucket
LIMIT 10
