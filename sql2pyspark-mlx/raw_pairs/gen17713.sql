SELECT a.id, a.country, a.rating FROM suppliers a WHERE a.rating < (SELECT AVG(b.rating) FROM suppliers b WHERE b.country = a.country)
