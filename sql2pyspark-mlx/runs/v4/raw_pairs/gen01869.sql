SELECT hire_date AS event_date
FROM employees
UNION
SELECT signup_date AS event_date
FROM customers
