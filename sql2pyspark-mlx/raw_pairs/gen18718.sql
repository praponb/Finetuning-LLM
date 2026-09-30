SELECT job_title, DATE_TRUNC('MONTH', hire_date) hire_month, ROUND(STDDEV(salary), 2) stddev_salary FROM employees WHERE job_title IS NOT NULL GROUP BY 1, 2 ORDER BY hire_month;
