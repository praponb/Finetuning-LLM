SELECT opened_date
FROM stores
WHERE city = 'Chiang Mai'
UNION
SELECT hire_date
FROM employees
WHERE manager_id IN (26, 33)
