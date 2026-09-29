SELECT launch_date
FROM products
WHERE MONTH(launch_date) IN (2, 3, 7)
INTERSECT
SELECT hire_date
FROM employees
WHERE LENGTH(full_name) > 12
ORDER BY launch_date;
