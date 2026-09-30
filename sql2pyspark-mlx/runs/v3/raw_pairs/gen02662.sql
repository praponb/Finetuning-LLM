SELECT country FROM suppliers WHERE city = 'New York' INTERSECT SELECT country FROM suppliers WHERE supplier_name IS NOT NULL;
