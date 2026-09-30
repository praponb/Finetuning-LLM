SELECT CASE WHEN salary >= 129000 THEN 'high' WHEN salary >= 77000 THEN 'mid' ELSE 'low' END AS salary_bucket, SUM(CASE WHEN level = 'L1' THEN salary ELSE 0 END) AS l1_salary, ROUND(STDDEV(salary), 2) AS stddev_salary
FROM employees
WHERE hire_date BETWEEN '2024-04-26' AND '2024-08-01'
GROUP BY 1
ORDER BY stddev_salary
