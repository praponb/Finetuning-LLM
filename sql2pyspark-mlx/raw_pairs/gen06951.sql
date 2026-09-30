SELECT id, country, rating, supplier_name FROM suppliers WHERE supplier_name LIKE 'Pa%' AND city NOT IN ('New York', 'Tokyo') AND supplier_name LIKE 'P%' ORDER BY supplier_name, id LIMIT 5;
