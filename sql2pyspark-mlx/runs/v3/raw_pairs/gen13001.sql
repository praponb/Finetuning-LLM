SELECT supplier_name FROM suppliers WHERE supplier_name LIKE 'S%' INTERSECT SELECT supplier_name FROM suppliers WHERE rating > 1.8 ORDER BY supplier_name;
