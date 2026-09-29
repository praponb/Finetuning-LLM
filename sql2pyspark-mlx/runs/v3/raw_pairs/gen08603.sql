SELECT id, email, DATE_FORMAT(hire_date, 'MMM yyyy') AS hire_date_label
FROM employees
WHERE LOWER(email) LIKE '%mma%' AND email NOT LIKE '%@company.co';
