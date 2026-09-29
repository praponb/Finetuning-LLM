SELECT CASE WHEN salary >= 150000 THEN 'high' WHEN salary >= 43000 THEN 'mid' ELSE 'low' END AS salary_bucket, AVG(salary) AS avg_salary, SORT_ARRAY(COLLECT_SET(level)) AS level_list
FROM employees
WHERE email IS NULL
GROUP BY CASE WHEN salary >= 150000 THEN 'high' WHEN salary >= 43000 THEN 'mid' ELSE 'low' END
ORDER BY avg_salary DESC;
