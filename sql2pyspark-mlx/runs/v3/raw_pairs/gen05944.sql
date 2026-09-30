SELECT id, country, rating, supplier_name, FIRST_VALUE(supplier_name) OVER (PARTITION BY country ORDER BY rating, id) AS first_supplier_name FROM suppliers WHERE country <> 'SG';
