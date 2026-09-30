SELECT id, age, country FROM customers WHERE age >= (SELECT AVG(age) FROM customers) ORDER BY country DESC, id DESC;
