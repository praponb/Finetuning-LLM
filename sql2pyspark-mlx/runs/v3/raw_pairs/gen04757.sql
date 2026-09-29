SELECT brand, MAX(stock_qty) - MIN(stock_qty) stock_qty_range FROM products GROUP BY 1
