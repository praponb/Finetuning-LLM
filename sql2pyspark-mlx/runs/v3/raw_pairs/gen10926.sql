SELECT id, brand, launch_date, stock_qty, LAG(stock_qty, 2, 0) OVER (PARTITION BY brand ORDER BY launch_date, id) AS prev_stock_qty FROM products
