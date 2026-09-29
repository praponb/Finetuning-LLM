SELECT country, supplier_name FROM suppliers WHERE rating < 3.7 UNION SELECT country, supplier_name FROM suppliers WHERE LOWER(supplier_name) LIKE '%aci%';
