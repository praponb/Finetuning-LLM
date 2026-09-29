SELECT ROUND(AVG(salary), 2) AS avg_salary FROM employees WHERE email LIKE '%@GMAIL.COM' ORDER BY avg_salary DESC;
