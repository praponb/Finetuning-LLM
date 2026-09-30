SELECT id, city, country FROM suppliers WHERE rating <= 2.7 UNION SELECT id, city, country FROM suppliers WHERE rating >= 2.8;
