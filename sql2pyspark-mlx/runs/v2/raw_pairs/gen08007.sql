SELECT s.supplier_name, s.country, p.supplier_id FROM suppliers s LEFT OUTER JOIN products p ON s.id = p.supplier_id WHERE s.supplier_name IS NOT NULL ORDER BY country DESC;
