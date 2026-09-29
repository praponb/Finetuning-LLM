SELECT brand, COUNT(stock_qty) AS stock_qty_count FROM products WHERE product_name LIKE 'Run%' GROUP BY brand;
