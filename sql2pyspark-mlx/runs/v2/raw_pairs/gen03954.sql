SELECT * FROM suppliers WHERE LENGTH(supplier_name) < 12 AND (rating > 2.3 OR country <> 'JP') ORDER BY rating;
