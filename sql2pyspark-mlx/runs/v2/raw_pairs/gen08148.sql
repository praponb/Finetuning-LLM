SELECT id, supplier_name, rating, CASE WHEN country = 'DE' THEN 1 ELSE 0 END is_de FROM suppliers WHERE (rating IS NOT NULL AND country IN ('DE', 'US')) OR city IN ('Singapore', 'Berlin')
