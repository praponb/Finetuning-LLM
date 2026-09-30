SELECT DATE_FORMAT(hire_date, 'yyyy-MM') AS hire_month, COUNT(CASE WHEN job_title = 'Intern' THEN 1 END) AS intern_count
FROM employees
WHERE email IS NULL
GROUP BY 1;
