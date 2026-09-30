SELECT id, city, supplier_name, CONCAT(city, ', ', supplier_name) AS city_supplier_name FROM suppliers WHERE city != 'London' AND UPPER(supplier_name) LIKE '%LOB%'
