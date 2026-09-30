SELECT id, age, segment FROM customers WHERE age >= (SELECT AVG(age) FROM customers)
