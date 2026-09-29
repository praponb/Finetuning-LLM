SELECT *, DATE_FORMAT(hire_date, 'yyyy-MM') AS hire_date_label, FLOOR(salary) AS salary_whole FROM employees WHERE hire_date > '2025-07-07' AND manager_id = 12
