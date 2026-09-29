SELECT id, age FROM customers WHERE age <= (SELECT MIN(age) FROM customers) ORDER BY age;
