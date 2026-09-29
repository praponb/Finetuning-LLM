SELECT level, ROUND(STDDEV(salary), 2) AS stddev_salary, SUM(CASE WHEN job_title = 'Manager' THEN 1 ELSE 0 END) AS manager_count
FROM employees
GROUP BY level
ORDER BY stddev_salary DESC;
