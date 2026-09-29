SELECT QUARTER(hire_date) AS hire_quarter, COUNT(*) AS num_employees
FROM employees
GROUP BY QUARTER(hire_date)
ORDER BY 2 DESC, hire_quarter ASC
LIMIT 50;
