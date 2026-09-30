SELECT dept_name
FROM departments
WHERE budget >= 3650000
EXCEPT
SELECT country
FROM suppliers
WHERE country = 'US';
