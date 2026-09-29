SELECT job_title, MAX(hire_date) latest_hire_date FROM employees WHERE salary <= 126000 GROUP BY 1 ORDER BY latest_hire_date;
