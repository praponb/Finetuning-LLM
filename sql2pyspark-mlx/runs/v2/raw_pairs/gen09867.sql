SELECT CASE WHEN salary >= 126000 THEN 'high' WHEN salary >= 42000 THEN 'mid' ELSE 'low' END salary_bucket, SUM(salary) total_salary, DENSE_RANK() OVER (ORDER BY SUM(salary) DESC) AS rnk
FROM employees
GROUP BY CASE WHEN salary >= 126000 THEN 'high' WHEN salary >= 42000 THEN 'mid' ELSE 'low' END;
