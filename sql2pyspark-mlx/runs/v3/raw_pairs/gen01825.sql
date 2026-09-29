SELECT CASE WHEN salary >= 133000 THEN 'high' WHEN salary >= 83000 THEN 'mid' ELSE 'low' END AS salary_bucket, YEAR(hire_date) AS hire_year, MAX(salary) - MIN(salary) AS salary_range
FROM employees
GROUP BY CASE WHEN salary >= 133000 THEN 'high' WHEN salary >= 83000 THEN 'mid' ELSE 'low' END, YEAR(hire_date)
HAVING MAX(salary) - MIN(salary) >= 24
