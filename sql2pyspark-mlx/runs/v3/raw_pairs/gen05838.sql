SELECT id, supplier_name, rating, city, CONCAT(country, ' ', supplier_name) country_supplier_name FROM suppliers WHERE supplier_name NOT LIKE '%Import%' AND rating <= 4.1;
