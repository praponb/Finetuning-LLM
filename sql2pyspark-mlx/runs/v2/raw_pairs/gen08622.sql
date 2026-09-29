SELECT s.supplier_name, p.stock_qty FROM suppliers s INNER JOIN products p ON p.supplier_id = s.id ORDER BY supplier_name;
