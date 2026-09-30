SELECT s.supplier_name, p.stock_qty FROM suppliers s JOIN products p ON s.id = p.supplier_id ORDER BY supplier_name
