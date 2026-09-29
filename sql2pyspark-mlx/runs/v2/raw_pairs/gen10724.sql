SELECT CASE WHEN salary >= 138000 THEN 'high' WHEN salary >= 81000 THEN 'mid' ELSE 'low' END salary_bucket, MAX(hire_date) last_hire_date
FROM employees
WHERE salary BETWEEN 90000 AND 105000 OR level IN ('L2', 'L3')
GROUP BY salary_bucket
