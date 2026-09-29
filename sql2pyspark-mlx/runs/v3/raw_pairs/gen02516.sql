SELECT id, country FROM suppliers WHERE LOWER(TRIM(supplier_name)) = 'lion city imports' INTERSECT SELECT id, country FROM suppliers WHERE rating BETWEEN 3.5 AND 4.1;
