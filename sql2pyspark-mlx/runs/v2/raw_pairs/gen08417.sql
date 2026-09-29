SELECT level, ROUND(STDDEV(salary), 2) stddev_salary
FROM employees
WHERE LENGTH(email) < 9
GROUP BY level
ORDER BY stddev_salary DESC;
