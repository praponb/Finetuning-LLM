SELECT s.id, s.country FROM suppliers s WHERE s.country IN ('UK', 'SG', 'US') AND NOT EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id AND p.brand IN ('Stark', 'Umbrella', 'Acme'));
