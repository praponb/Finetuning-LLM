SELECT id, country FROM suppliers INTERSECT ALL SELECT id, country FROM suppliers WHERE city IS NULL
