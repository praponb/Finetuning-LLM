SELECT country, supplier_name, rating, COALESCE(supplier_name, 'Unknown') AS supplier_name_filled, CONCAT_WS(',', country, supplier_name) AS country_supplier_name_key FROM suppliers
