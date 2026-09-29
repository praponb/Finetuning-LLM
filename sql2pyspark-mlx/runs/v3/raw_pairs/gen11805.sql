SELECT *
FROM employees
WHERE LOWER(TRIM(email)) = 'alice@gmail.com'
