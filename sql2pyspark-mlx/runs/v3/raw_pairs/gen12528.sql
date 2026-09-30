SELECT p.id, p.stock_qty, s.supplier_name, s.city, s.country FROM products p FULL OUTER JOIN suppliers s ON s.id = p.supplier_id WHERE s.rating >= 3.6 ORDER BY city ASC;
