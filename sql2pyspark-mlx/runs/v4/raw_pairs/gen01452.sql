SELECT id, supplier_name, country FROM suppliers WHERE rating < 3.6 UNION ALL SELECT id, supplier_name, country FROM suppliers WHERE rating BETWEEN 2.8 AND 3.5 ORDER BY country;
