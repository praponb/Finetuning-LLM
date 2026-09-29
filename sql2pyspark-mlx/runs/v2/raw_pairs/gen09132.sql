SELECT id, supplier_name, COALESCE(supplier_name, 'N/A') AS supplier_name_filled FROM suppliers WHERE country IN ('JP', 'SG', 'DE') AND city = 'New York' AND country IN ('UK', 'DE');
