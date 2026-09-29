SELECT c.id, c.country, c.age FROM customers c WHERE c.age = (SELECT MAX(c2.age) FROM customers c2 WHERE c2.country = c.country) ORDER BY c.country;
