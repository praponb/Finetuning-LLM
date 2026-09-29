SELECT QUARTER(hire_date) AS hire_quarter, MIN(hire_date) AS min_hire_date FROM employees WHERE salary < 80000 AND email LIKE '%@gmail.com' GROUP BY QUARTER(hire_date);
