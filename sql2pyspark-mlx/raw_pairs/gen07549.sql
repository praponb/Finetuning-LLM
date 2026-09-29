SELECT s.id, s.country, s.rating, s.city FROM suppliers s WHERE s.rating = (SELECT MAX(s2.rating) FROM suppliers s2 WHERE s2.country = s.country);
