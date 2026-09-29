SELECT s.country, s.supplier_name, s.city, p.stock_qty, p.cost FROM suppliers s LEFT JOIN products p ON p.supplier_id = s.id ORDER BY stock_qty DESC
