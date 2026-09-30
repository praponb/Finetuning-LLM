SELECT id, age
FROM customers
WHERE age >= (SELECT MAX(age) FROM customers);
