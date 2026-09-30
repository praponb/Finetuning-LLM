SELECT job_title, level, SUM(CASE WHEN job_title = 'Manager' THEN 1 ELSE 0 END) AS manager_count, ROUND(STDDEV(salary), 2) AS stddev_salary FROM employees GROUP BY job_title, level
