SELECT a.id, a.country, a.age, a.email FROM customers a WHERE a.age = (SELECT MAX(b.age) FROM customers b WHERE b.country = a.country) ORDER BY a.country;
