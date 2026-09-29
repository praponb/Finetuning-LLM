SELECT dept_id, hire_date, DATE_TRUNC('MONTH', hire_date) AS hire_date_period FROM employees WHERE level IS NULL AND hire_date BETWEEN '2023-03-01' AND '2024-12-01' AND full_name NOT LIKE 'Oli%';
