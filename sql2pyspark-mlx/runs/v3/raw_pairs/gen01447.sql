SELECT opened_date AS event_date
FROM stores
INTERSECT
SELECT hire_date AS event_date
FROM employees;
