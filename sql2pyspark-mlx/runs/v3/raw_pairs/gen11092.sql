SELECT a.id, a.country, a.rating, a.city FROM suppliers a WHERE a.rating = (SELECT MIN(b.rating) FROM suppliers b WHERE b.country = a.country) ORDER BY a.id DESC
