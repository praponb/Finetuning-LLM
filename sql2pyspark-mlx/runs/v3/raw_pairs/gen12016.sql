SELECT s.rating, p.launch_date, p.category, p.supplier_id FROM suppliers s LEFT OUTER JOIN products p ON p.supplier_id = s.id WHERE s.rating IS NULL
