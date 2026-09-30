SELECT id, rating, country, city FROM suppliers WHERE rating = (SELECT MIN(rating) FROM suppliers)
