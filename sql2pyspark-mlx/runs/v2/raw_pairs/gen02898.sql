SELECT job_title, ROUND(STDDEV(salary), 2) AS stddev_salary FROM employees WHERE full_name IS NOT NULL AND is_remote GROUP BY job_title ORDER BY stddev_salary DESC, job_title LIMIT 50;
