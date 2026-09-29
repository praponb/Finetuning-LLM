SELECT id, supplier_name, rating, city, CONCAT(country, ' - ', supplier_name) AS country_supplier_name FROM suppliers WHERE NOT (rating < 2.0 AND country <> 'SG' AND country IN ('DE', 'SG', 'UK'));
