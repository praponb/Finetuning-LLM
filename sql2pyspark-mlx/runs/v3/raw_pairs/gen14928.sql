SELECT s.city, s.country, p.stock_qty, p.launch_date FROM suppliers s LEFT JOIN products p ON p.supplier_id = s.id
