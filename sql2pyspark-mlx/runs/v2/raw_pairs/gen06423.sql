SELECT city FROM suppliers WHERE country = 'TH' INTERSECT SELECT city FROM suppliers WHERE rating < 2.3 ORDER BY city;
