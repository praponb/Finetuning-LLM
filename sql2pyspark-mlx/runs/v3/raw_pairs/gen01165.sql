SELECT hire_date, 'employee' AS source
FROM employees
UNION ALL
SELECT signup_date AS hire_date, 'customer' AS source
FROM customers
WHERE country IN ('DE', 'UK', 'US')
ORDER BY hire_date
