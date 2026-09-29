SELECT a.id, a.country, a.age, a.name FROM customers a WHERE a.age > (SELECT AVG(b.age) FROM customers b WHERE b.country = a.country);
