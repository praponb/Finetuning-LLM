SELECT s.id, s.supplier_name, t.min_stock_qty FROM suppliers s INNER JOIN (SELECT supplier_id, MIN(stock_qty) AS min_stock_qty FROM products GROUP BY supplier_id) t ON s.id = t.supplier_id;
