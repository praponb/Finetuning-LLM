SELECT id, city FROM suppliers EXCEPT SELECT id, city FROM suppliers WHERE rating < 3.9
