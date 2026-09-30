SELECT s.id, s.country, s.supplier_name FROM suppliers s WHERE s.country IN ('US', 'SG') AND s.id IN (SELECT supplier_id FROM products WHERE ARRAY_CONTAINS(tags, 'eco'));
