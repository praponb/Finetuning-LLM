SELECT s.country, s.rating, s.city, p.stock_qty, p.price, p.launch_date FROM suppliers s LEFT OUTER JOIN products p ON s.id = p.supplier_id ORDER BY s.rating, p.launch_date;
