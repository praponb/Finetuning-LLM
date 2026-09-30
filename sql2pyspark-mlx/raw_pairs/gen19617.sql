SELECT s.supplier_name, s.country, p.brand, p.price FROM suppliers s LEFT OUTER JOIN products p ON p.supplier_id = s.id WHERE p.cost >= 650 OR SIZE(p.tags) > 1
