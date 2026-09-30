SELECT job_title, MAX(hire_date) AS latest_hire_date
FROM employees
GROUP BY job_title;
