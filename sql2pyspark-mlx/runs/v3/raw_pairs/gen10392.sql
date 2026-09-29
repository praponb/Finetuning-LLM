SELECT s.rating, s.supplier_name, s.country, p.brand FROM suppliers s LEFT OUTER JOIN products p ON s.id = p.supplier_id
