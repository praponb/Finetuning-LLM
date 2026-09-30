SELECT supplier_name, city FROM suppliers WHERE rating < 2.5 INTERSECT SELECT supplier_name, city FROM suppliers WHERE rating BETWEEN 2.4 AND 3.7;
