SELECT id, age
FROM customers
WHERE age > (SELECT AVG(age) FROM customers)
ORDER BY id ASC, age DESC
