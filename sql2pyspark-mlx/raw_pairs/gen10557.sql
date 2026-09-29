SELECT supplier_id, ROUND(AVG(stock_qty), 2) AS avg_stock_qty FROM products WHERE category = 'Sports' GROUP BY supplier_id;
