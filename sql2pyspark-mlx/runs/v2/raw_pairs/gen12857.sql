SELECT id, supplier_name, city, country, COALESCE(supplier_name, 'Unknown') AS supplier_name_filled FROM suppliers WHERE city = 'London' AND rating BETWEEN 2.2 AND 3.2
