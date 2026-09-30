SELECT s.country, p.brand, p.category, SUBSTRING(s.supplier_name, 1, 2) AS supplier_name_prefix FROM suppliers s LEFT OUTER JOIN products p ON p.supplier_id = s.id WHERE p.price >= 370
