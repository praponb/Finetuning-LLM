SELECT order_date
FROM orders
WHERE order_date IS NOT NULL
UNION
SELECT hire_date AS order_date
FROM employees
WHERE hire_date IS NOT NULL
