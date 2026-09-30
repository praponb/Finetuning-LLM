SELECT dept_id, ROUND(STDDEV(salary), 2) AS stddev_salary
FROM employees
GROUP BY dept_id;
