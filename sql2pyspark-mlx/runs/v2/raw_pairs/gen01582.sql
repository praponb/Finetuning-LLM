SELECT level, CASE WHEN salary >= 123000 THEN 'high' WHEN salary >= 57000 THEN 'mid' ELSE 'low' END AS salary_bucket, SUM(salary) AS total_salary
FROM employees
WHERE LOWER(TRIM(full_name)) = 'kanya srisuk'
GROUP BY level, CASE WHEN salary >= 123000 THEN 'high' WHEN salary >= 57000 THEN 'mid' ELSE 'low' END
ORDER BY level ASC, salary_bucket ASC
LIMIT 10
