SELECT id, rating FROM suppliers WHERE rating > (SELECT MAX(rating) FROM suppliers WHERE rating BETWEEN 2.9 AND 4.1) ORDER BY id DESC
