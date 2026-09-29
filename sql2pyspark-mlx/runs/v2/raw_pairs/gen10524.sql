SELECT country, supplier_name FROM suppliers WHERE rating > 3.1 UNION SELECT country, supplier_name FROM suppliers WHERE rating >= 3.0 ORDER BY country
