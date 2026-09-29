SELECT sale_date AS event_date
FROM store_sales
UNION
SELECT hire_date AS event_date
FROM employees
WHERE dept_id IN (7, 12)
ORDER BY event_date;
