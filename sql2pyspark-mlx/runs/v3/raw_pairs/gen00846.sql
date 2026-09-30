SELECT s.rating, s.country, p.cost, p.stock_qty, p.price, CONCAT(s.supplier_name, ' - ', s.country) supplier_name_country FROM suppliers s LEFT JOIN products p ON p.supplier_id = s.id;
