SELECT supplier_name, country FROM suppliers WHERE rating < 2.4 UNION ALL SELECT supplier_name, country FROM suppliers WHERE city = 'Singapore'
