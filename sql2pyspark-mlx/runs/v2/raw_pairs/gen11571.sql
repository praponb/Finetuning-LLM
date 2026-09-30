SELECT s.id, s.rating, s.city, t.avg_stock_qty FROM suppliers s INNER JOIN (SELECT supplier_id, ROUND(AVG(stock_qty), 2) avg_stock_qty FROM products GROUP BY supplier_id) t ON s.id = t.supplier_id;
