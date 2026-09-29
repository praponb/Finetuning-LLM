SELECT s.country, p.category FROM suppliers s LEFT OUTER JOIN products p ON p.supplier_id = s.id
